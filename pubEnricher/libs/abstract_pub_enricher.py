#!/usr/bin/python

import sys
import os
import json
import configparser
import copy

from abc import abstractmethod

from typing import overload, Tuple, List, Dict, Any, Iterator

from .skeleton_pub_enricher import SkeletonPubEnricher
from .pub_cache import PubCache

from . import pub_common

class AbstractPubEnricher(SkeletonPubEnricher):
	DEFAULT_REQUEST_DELAY = 0.25
	DEFAULT_MAX_RETRIES = 5
	
	@overload
	def __init__(self,cache:str=".",prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	@overload
	def __init__(self,cache:PubCache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	def __init__(self,cache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		super().__init__(cache,prefix,config,debug)
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		request_delay = self.config.getfloat(section_name,'request_delay',fallback=self.DEFAULT_REQUEST_DELAY)
		self.request_delay = request_delay
		
		# Maximum number of retries
		self.max_retries = self.config.getint(section_name,'retries',fallback=self.DEFAULT_MAX_RETRIES)
	
	@classmethod
	@abstractmethod
	def Name(cls) -> str:
		return 'abstract'
	
	@classmethod
	def populateMapping(cls,base_mapping:Dict[str,Any],dest_mapping:Dict[str,Any],onlyYear:bool=False) -> None:
		if onlyYear:
			dest_mapping['year'] = base_mapping.get('year')
		else:
			dest_mapping.update(base_mapping)
	
	@abstractmethod
	def populatePubIdsBatch(self,partial_mappings:List[Dict[str,Any]]) -> None:
		pass
	
	def populatePubIds(self,partial_mappings:List[Dict[str,Any]],onlyYear:bool=False) -> None:
		populable_mappings = []
		
		for partial_mapping in partial_mappings:
			# We are interested only in the year facet
			# as it is a kind of indicator
			pubYear = partial_mapping.get('year')
			_id = partial_mapping.get('id')
			if _id is not None and (pubYear is None or not onlyYear):
				mapping = self.pubC.getCachedMapping(partial_mapping['source'],_id)
				
				if mapping is None:
					populable_mappings.append(partial_mapping)
				else:
					self.populateMapping(mapping,partial_mapping,onlyYear)
		
		if len(populable_mappings) > 0:
			for start in range(0,len(populable_mappings),self.step_size):
				stop = start+self.step_size
				populable_mappings_slice = populable_mappings[start:stop]
				populable_mappings_clone_slice = list(map(lambda p_m: { 'id': p_m['id'], 'source': p_m['source'] } , populable_mappings_slice))
				self.populatePubIdsBatch(populable_mappings_clone_slice)
			
				for p_m,p_m_c in zip(populable_mappings_slice,populable_mappings_clone_slice):
					# It is a kind of indicator the 'year' flag
					if p_m_c.get('year') is not None:
						self.pubC.setCachedMapping(p_m_c)
						self.populateMapping(p_m_c,p_m,onlyYear)
			
	
	@abstractmethod
	def queryPubIdsBatch(self,query_ids:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		pass
	
	def reconcilePubIdsBatch(self,entries:List[Any]) -> None:
		# First, gather all the ids on one list, prepared for the query
		# MED: prefix has been removed because there are some problems
		# on the server side
		
		p2e = {}
		pmc2e = {}
		d2e = {}
		pubmed_pairs = []
		
		def _updateCaches(publish_id:str) -> bool:
			internal_ids = self.pubC.getSourceIds(publish_id)
			if internal_ids is not None:
				for source_id,_id in internal_ids:
					mapping = self.pubC.getCachedMapping(source_id,_id)
					pubmed_pairs.append(mapping)
					
					source_id = mapping['source']
					
					pubmed_id = mapping.get('pmid')
					if pubmed_id is not None:
						p2e.setdefault(pubmed_id,{})[source_id] = mapping
					
					doi_id = mapping.get('doi')
					if doi_id is not None:
						doi_id_norm = pub_common.normalize_doi(doi_id)
						d2e.setdefault(doi_id_norm,{})[source_id] = mapping
					
					pmc_id = mapping.get('pmcid')
					if pmc_id is not None:
						pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
						pmc2e.setdefault(pmc_id_norm,{})[source_id] = mapping
				
				return True
			else:
				return False
		
		# Preparing the query ids
		query_ids = []
		# This set allows avoiding to issue duplicate queries
		set_query_ids = set()
		for entry_pubs in map(lambda entry: entry['entry_pubs'],entries):
			for entry_pub in entry_pubs:
				query_id = {}
				# This loop avoid resolving twice
				pubmed_id = entry_pub.get('pmid')
				pubmed_set_id = (pubmed_id,'pmid')
				if pubmed_id is not None and pubmed_set_id not in set_query_ids and pubmed_id not in p2e:
					if not _updateCaches(pubmed_id):
						set_query_ids.add(pubmed_set_id)
						query_id['pmid'] = pubmed_id
						
				
				doi_id = entry_pub.get('doi')
				if doi_id is not None:
					doi_id_norm = pub_common.normalize_doi(doi_id)
					doi_set_id = (doi_id_norm,'doi')
					if doi_set_id not in set_query_ids and doi_id_norm not in d2e and not _updateCaches(doi_id_norm):
						set_query_ids.add(doi_set_id)
						query_id['doi'] = doi_id_norm
				
				pmc_id = entry_pub.get('pmcid')
				if pmc_id is not None:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					pmc_set_id = (pmc_id_norm,'pmcid')
					if pmc_set_id not in set_query_ids and pmc_id_norm not in pmc2e and not _updateCaches(pmc_id_norm):
						set_query_ids.add(pmc_set_id)
						query_id['pmcid'] = pmc_id_norm
				
				# Add it when there is something to query about
				if len(query_id) > 0:
					query_ids.append(query_id)
		
		# Now, with the unknown ones, let's ask the server
		if len(query_ids) > 0:
			try:
				gathered_pubmed_pairs = self.queryPubIdsBatch(query_ids)
				
				# Cache management
				for mapping in gathered_pubmed_pairs:
					_id = mapping['id']
					source_id = mapping['source']
					self.pubC.setCachedMapping(mapping)
					
					pubmed_id = mapping.get('pmid')
					if pubmed_id is not None:
						p2e.setdefault(pubmed_id,{})[source_id] = mapping
					
					pmc_id = mapping.get('pmcid')
					if pmc_id is not None:
						pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
						pmc2e.setdefault(pmc_id_norm,{})[source_id] = mapping
					
					doi_id = mapping.get('doi')
					if doi_id is not None:
						doi_id_norm = pub_common.normalize_doi(doi_id)
						d2e.setdefault(doi_id_norm,{})[source_id] = mapping
					
					pubmed_pairs.append(mapping)

					# print(json.dumps(entries,indent=4))
				# sys.exit(1)
			except Exception as anyEx:
				print("Something unexpected happened",file=sys.stderr)
				print(anyEx,file=sys.stderr)
				raise anyEx
		
		# Reconciliation and checking missing ones
		for entry in entries:
			for entry_pub in entry['entry_pubs']:
				broken_curie_ids = []
				initial_curie_ids = []
				
				results = []
				pubmed_id = entry_pub.get('pmid')
				if pubmed_id is not None:
					curie_id = pub_common.pmid2curie(pubmed_id)
					initial_curie_ids.append(curie_id)
					if pubmed_id in p2e:
						results.append(p2e[pubmed_id])
					else:
						broken_curie_ids.append(curie_id)
				
				doi_id = entry_pub.get('doi')
				if doi_id is not None:
					curie_id = pub_common.doi2curie(doi_id)
					initial_curie_ids.append(curie_id)
					doi_id_norm = pub_common.normalize_doi(doi_id)
					if doi_id_norm in d2e:
						results.append(d2e[doi_id_norm])
					else:
						broken_curie_ids.append(curie_id)
				
				pmc_id = entry_pub.get('pmcid')
				if pmc_id is not None:
					curie_id = pub_common.pmcid2curie(pmc_id)
					initial_curie_ids.append(curie_id)
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					if pmc_id_norm in pmc2e:
						results.append(pmc2e[pmc_id_norm])
					else:
						broken_curie_ids.append(curie_id)
				
				# Checking all the entries at once
				winner_set = None
				notFound = len(results) == 0
				for result in results:
					if winner_set is None:
						winner_set = result
					elif winner_set != result:
						winner = None
						break
				
				winners = []
				if winner_set is not None:
					for winner in iter(winner_set.values()):
						# Duplicating in order to augment it
						new_winner = copy.deepcopy(winner)
						
						curie_ids = []
						
						pubmed_id = new_winner.get('pmid')
						if pubmed_id is not None:
							curie_id = pub_common.pmid2curie(pubmed_id)
							curie_ids.append(curie_id)
						
						doi_id = new_winner.get('doi')
						if doi_id is not None:
							curie_id = pub_common.doi2curie(doi_id)
							curie_ids.append(curie_id)
						
						pmc_id = new_winner.get('pmcid')
						if pmc_id is not None:
							curie_id = pub_common.pmcid2curie(pmc_id)
							curie_ids.append(curie_id)
						
						new_winner['curie_ids'] = curie_ids
						new_winner['broken_curie_ids'] = broken_curie_ids
						winners.append(new_winner)
				else:
					broken_winner = {
						'id': None,
						'source': None,
						'curie_ids': initial_curie_ids,
						'broken_curie_ids': broken_curie_ids,
						'pmid': pubmed_id,
						'doi': doi_id,
						'pmcid': pmc_id
					}
					# No possible result
					if notFound:
						broken_winner['reason'] = 'notFound' if len(initial_curie_ids) > 0  else 'noReference'
					# There were mismatches
					else:
						broken_winner['reason'] = 'mismatch'
					
					winners.append(broken_winner)
				
				entry_pub['found_pubs'].extend(winners)
	
	@abstractmethod
	def queryCitRefsBatch(self,query_citations_data:Iterator[Dict[str,Any]]) -> Iterator[Dict[str,Any]]:
		pass
	
	
	def _reconcileCitRefMetricsBatch(self,query_citations_data:List[Dict[str,Any]],query_hash) -> None:
		# Update the cache with the new data
		if len(query_citations_data) > 0:
			try:
				new_citations = self.queryCitRefsBatch(query_citations_data)
			except Exception as anyEx:
				print("ERROR: Something went wrong",file=sys.stderr)
				print(anyEx,file=sys.stderr)
				raise anyEx
			
			for new_citation in new_citations:
				source_id = new_citation['source']
				_id = new_citation['id']
				
				if 'citations' in new_citation:
					citations = new_citation['citations']
					citation_count = new_citation['citation_count']
					# There are cases where no citation could be fetched
					if citations is not None:
						self.pubC.setCitationsAndCount(source_id,_id,citations,citation_count)
					for pub_field in query_hash[(_id,source_id)]:
						pub_field['citation_count'] = citation_count
						pub_field['citations'] = citations
				
				if 'references' in new_citation:
					references = new_citation['references']
					reference_count = new_citation['reference_count']
					if references is not None:
						self.pubC.setReferencesAndCount(source_id,_id,references,reference_count)
					for pub_field in query_hash[(_id,source_id)]:
						pub_field['reference_count'] = reference_count
						pub_field['references'] = references
	
	def listReconcileCitRefMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of found publications and it retrieves citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		
		query_citations_data = []
		query_hash = {}
		for pub_field in pub_list:
			_id = pub_field.get('id') #11932250
			if _id is not None:
				source_id = pub_field['source']
				
				citations, citation_count = self.pubC.getCitationsAndCount(source_id,_id)
				if citations is not None:
					# Save now
					pub_field['citation_count'] = citation_count
					pub_field['citations'] = citations

				references, reference_count = self.pubC.getReferencesAndCount(source_id,_id)
				if references is not None:
					# Save now
					pub_field['reference_count'] = reference_count
					pub_field['references'] = references
				
				# Query later, without repetitions
				if citations is None or references is None:
					query_list = query_hash.setdefault((_id,source_id),[])
					if len(query_list) == 0:
						query_citations_data.append(pub_field)
					query_list.append(pub_field)
		
		# Update the cache with the new data
		self._reconcileCitRefMetricsBatch(query_citations_data,query_hash)
		
		# If we have to return the digested stats, compute them here
		if verbosityLevel<=0:
			for pub_field in pub_list:
				citations = pub_field.pop('citations',[])
				# Computing the stats
				pub_field['citation_stats'] = self._citrefStats(citations)
				
				references = pub_field.pop('references',[])
				# Computing the stats
				pub_field['reference_stats'] = self._citrefStats(references)
		elif verbosityLevel > 1:
			for pub_field in pub_list:
				citations = pub_field.get('citations')
				if citations is not None:
					self.populatePubIds(citations)
					
				references = pub_field.get('references')
				if references is not None:
					self.populatePubIds(references)
				
				if verbosityLevel >=2:
					self.listReconcileCitRefMetricsBatch(citations,verbosityLevel-1)
	
