#!/usr/bin/python

import sys
import os
import json

from abc import ABC, abstractmethod

from typing import overload, Tuple, List, Dict, Any

from .pub_cache import PubCache

from . import pub_common

class AbstractPubEnricher(ABC):
	DEFAULT_STEP_SIZE = 50
	
	@overload
	def __init__(self,cache:str="."):
		...
	
	@overload
	def __init__(self,cache:PubCache):
		...
	
	def __init__(self,cache):
		if type(cache) is str:
			self.cache_dir = cache
			self.pubC = PubCache(self.cache_dir)
		else:
			self.pubC = cache
			self.cache_dir = cache.cache_dir
		
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		super().__init__()
	
	def __enter__(self):
		self.pubC.__enter__()
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.pubC.__exit__(exc_type, exc_val, exc_tb)
	
	
	def populateMapping(base_mapping:Dict[str,Any],dest_mapping:Dict[str,Any],onlyYear:bool=False) -> None:
		if onlyYear:
			dest_mapping['year'] = base_mapping.get('year')
		else:
			for key,value in base_mapping.iter():
				dest_mapping[key] = value
	
	@abstractmethod
	def populatePubIdsBatch(self,partial_mappings:List[Dict[str,Any]]) -> None:
		pass
	
	def populatePubIds(self,mappings:List[Dict[str,Any]],onlyYear:bool=False) -> None:
		populable_mappings = []
		if onlyYear:
			# We are interested only in the year facet
			for partial_mapping in partial_mappings:
				pubYear = partial_mapping.get('year')
				if pubYear is None:
					populable_mappings.append(partial_mapping)
		else:
			for partial_mapping in partial_mappings:
				mapping = self.pubC.getCachedMapping(source_id,_id)
				
				if mapping is None:
					populable_mappings.append(partial_mapping)
				else:
					self.populateMapping(mapping,partial_mapping,onlyYear)
		
		if len(populable_mappings) > 0:
			populable_mappings_clone = list(map(lambda p_m: { 'id': p_m['id'], 'source': p_m['source'] } , populable_mappings))
			self.populatePubIdsBatch(populable_mappings_clone)
			for p_m,p_m_c in zip(populable_mappings,populable_mappings_clone):
				# It is a kind of indicator the 'year' flag
				if p_m_c.get('year') is not None:
					internal_ids = self.pubC.getSourceIds(publish_id)
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
						pmc2e.setdefault(pmc_id,{})[source_id] = mapping
				
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
				pmc_set_id = (pmc_id,'pmcid')
				if pmc_id is not None and pmc_set_id not in set_query_ids and pmc_id not in pmc2e:
					if not _updateCaches(pmc_id):
						set_query_ids.add(pmc_set_id)
						query_id['pmcid'] = pmc_id
				
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
						pmc2e.setdefault(pmc_id,{})[source_id] = mapping
					
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
					if pmc_id in pmc2e:
						results.append(pmc2e[pmc_id])
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
						new_winner = dict(winner)
						
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
	def queryCitationsBatch(self,query_citations_data:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		pass
	
	def reconcileCitationMetricsBatch(self,entries:List[Dict[str,Any]],digestStats:bool=True) -> None:
		"""
			This method takes in batches of entries and retrives citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		
		query_citations_data = []
		query_hash = {}
		for entry in entries:
			for entry_pub in entry['entry_pubs']:
				if entry_pub['found_pubs'] is not None:
					for pub_field in entry_pub['found_pubs']:
						_id = pub_field.get('id') #11932250
						if _id is not None:
							source_id = pub_field['source']
							citations, citations_count = self.pubC.getCitationsAndCount(source_id,_id)
							
							if citations is None:
								# Query later, without repetitions
								query_list = query_hash.setdefault((_id,source_id),[])
								if len(query_list) == 0:
									query_citations_data.append(pub_field)
								query_list.append(pub_field)
							else:
								# Save now
								pub_field['citation_count'] = citations_count
								pub_field['citations'] = citations
		
		# Update the cache with the new data
		if len(query_citations_data) > 0:
			try:
				new_citations = self.queryCitationsBatch(query_citations_data)
			except Exception as anyEx:
				print("ERROR: Something went wrong",file=sys.stderr)
				print(anyEx,file=sys.stderr)
				raise anyEx
			
			for new_citation in new_citations:
				source_id = new_citation['source']
				_id = new_citation['id']
				citations = new_citation.get('citations',[])
				citations_count = new_citation.get('citation_count',len(citations))
				self.pubC.setCitationsAndCount(source_id,_id,citations,citations_count)
				for pub_field in query_hash[(_id,source_id)]:
					pub_field['citation_count'] = citations_count
					pub_field['citations'] = citations
		
		# If we have to return the digested stats, compute them here
		if digestStats:
			for entry in entries:
				for entry_pub in entry['entry_pubs']:
					found_pubs = entry_pub.get('found_pubs')
					if found_pubs is not None:
						for pub_field in found_pubs:
							citations = pub_field.get('citations')
							if citations is not None:
								# Computing the stats
								citation_stats = {}
								for citation in citations:
									year = citation['year']
									if year in citation_stats:
										citation_stats[year] += 1
									else:
										citation_stats[year] = 1
								pub_field['citation_stats'] = citation_stats
								del pub_field['citations']
	
	def reconcilePubIds(self,entries:List[Any],results_dir:str=None,digestStats:bool=True,step_size:int=DEFAULT_STEP_SIZE) -> List[Any]:
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'found_pubs' key, it returns the same
			parameter as input
		"""
		
		for start in range(0,len(entries),step_size):
			stop = start+step_size
			entries_slice = entries[start:stop]
			self.reconcilePubIdsBatch(entries_slice)
			self.reconcileCitationMetricsBatch(entries_slice,digestStats)
			self.pubC.sync()
			if results_dir is not None:
				for idx, entry in enumerate(entries_slice):
					dest_file = os.path.join(results_dir,'entry_'+str(start+idx)+'.json')
					with open(dest_file,mode="w",encoding="utf-8") as outentry:
						json.dump(entry,outentry,indent=4,sort_keys=True)
		
		return entries
