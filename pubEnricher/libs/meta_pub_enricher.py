#!/usr/bin/python

import sys
import json
import re
import configparser

from collections import OrderedDict
import copy

#from multiprocessing import Process, Queue, Lock
import threading
import queue

from typing import overload, Tuple, List, Dict, Any, Iterator

from .pub_cache import MetaPubCache, PubCache
from .skeleton_pub_enricher import SkeletonPubEnricher
from .europepmc_enricher import EuropePMCEnricher
from .pubmed_enricher import PubmedEnricher
from .wikidata_enricher import WikidataEnricher

from . import pub_common

def _thread_target(q, method, *args):
	# We are saving either the result or any fired exception
	try:
		q.put(method(*args))
	except BaseException as e:
		q.put(e)

def _thread_wrapper(method,*args):
	eq = queue.Queue()
	et = threading.Thread(
		target=_thread_target,
		args=(eq,method,*args)
	)
	et.start()
	
	return (et,eq)


class MetaEnricher(SkeletonPubEnricher):
	RECOGNIZED_BACKENDS = [ EuropePMCEnricher, PubmedEnricher, WikidataEnricher ]
	RECOGNIZED_BACKENDS_HASH = OrderedDict( ( (backend.Name(),backend) for backend in RECOGNIZED_BACKENDS ) )
	ATTR_BLACKSET = {
		'id',
		'source',
		'enricher',
		'curie_ids',
		'base_pubs',
		'broken_curie_ids',
		'citation_count',
		'citation_refs',
		'citation_stats',
		'citations',
		'reason',
		'reference_count',
		'reference_refs',
		'reference_stats',
		'references',
	}
	
	@overload
	def __init__(self,cache:str=".",prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	@overload
	def __init__(self,cache:PubCache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	def __init__(self,cache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		# Create the instances needed by this meta enricher
		use_enrichers_str = config.get(section_name,'use_enrichers',fallback=None)
		
		use_enrichers = use_enrichers_str.split(',')  if use_enrichers_str else self.RECOGNIZED_BACKENDS_HASH.keys()
		
		enrichers = OrderedDict()
		for enricher_name in use_enrichers:
			enricher_class = self.RECOGNIZED_BACKENDS_HASH.get(enricher_name)
			if enricher_class:
				compound_prefix = prefix + '_' + enricher_name if prefix else '_' + enricher_name
				# Each value is an instance of AbstractPubEnricher
				enrichers[enricher_name] = enricher_class(cache,compound_prefix,config,debug)
		
		# And last, the meta-enricher itself
		meta_prefix = prefix + section_name  if prefix else section_name
		meta_prefix += '-'.join(enrichers.keys())
		meta_prefix = '_' + meta_prefix + '_'
		
		# And the meta-cache
		if type(cache) is str:
			pubC = MetaPubCache(cache,prefix=meta_prefix)
		else:
			pubC = cache
		
		super().__init__(pubC,meta_prefix,config,debug)
		
		self.enrichers = enrichers
	
	def __enter__(self):
		super().__enter__()
		for enricher in self.enrichers.values():
			enricher.__enter__()
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		super().__exit__(exc_type, exc_val, exc_tb)
		for enricher in self.enrichers.values():
			enricher.__exit__(exc_type, exc_val, exc_tb)
		return self
	
	# Do not change this constant!!!
	META_SOURCE='meta'
	
	@classmethod
	def Name(cls) -> str:
		return cls.META_SOURCE
	
	# Specific methods
	def _initializeMergingEntry(self,entry:Dict[str,Any],entry_enricher:str,base_entry:Dict[str,Any]=None) -> Dict[str,Any]:
		"""
			This method initializes the structures used to gather the results from the different enrichers to be merged
		"""
		
		merged_entry = copy.deepcopy(entry)
		found_pubs = merged_entry.setdefault('found_pubs',[])
		for found_pub in found_pubs:
			found_pub['enricher'] = entry_enricher
		
		if base_entry is None:
			base_entry = merged_entry
		else:
			base_entry['found_pubs'].extend(found_pubs)
		
		#
		#print(entry,file=sys.stderr)
		#merged_entry = copy.deepcopy(entry)
		#merged_entry['base_entries'] = []
		#print(merged_entry,file=sys.stderr)
		#
		#if entry['id'] is not None:
		#	merged_entry['base_entries'].push(entry)
		#	
		#	result_tuple = (entry_enricher,entry['source'],entry['id'])
		#	merged_entry['id'] =  [ result_tuple ]
		#	merged_entry['source'] = self.META_SOURCE
		#	
		#	entry['enricher'] = entry_enricher
		
		return base_entry
	
	def _mergeFoundPubs(self,found_pubs:List[Dict[str,Any]]) -> Dict[str,Any]:
		"""
			This method takes an array of fetched entries, which have to be merged
			and it returns a merged entry
		"""
		
		merged_pub = None
		if found_pubs:
			base_pubs = []
			initial_curie_ids = []
			putative_ids = []
			# Step 1: initialize features
			merged_pub = {
				'source': self.META_SOURCE,
				'base_pubs': base_pubs,
				'curie_ids': initial_curie_ids,
			}
			for i_found_pub,base_found_pub in enumerate(found_pubs):
				if base_found_pub['id'] is not None:
					base_pubs.append({'id': base_found_pub['id'],'source': base_found_pub['source'],'enricher': base_found_pub['enricher']})
					for key,val in base_found_pub.items():
						# Should we skip specific fields?
						# This gives a chance to initialize an unknown field
						if (key in self.ATTR_BLACKSET) or (val is None):
							continue
						
						# TODO: conflict detection, when a source missets an identifier
						# Only interesting
						merged_pub.setdefault(key,val)
			
			pubmed_id = merged_pub.get('pmid')
			if pubmed_id is not None:
				curie_id = pub_common.pmid2curie(pubmed_id)
				initial_curie_ids.append(curie_id)
				putative_ids.append(pubmed_id)
			
			doi_id = merged_pub.get('doi')
			if doi_id is not None:
				curie_id = pub_common.doi2curie(doi_id)
				initial_curie_ids.append(curie_id)
				doi_id_norm = pub_common.normalize_doi(doi_id)
				putative_ids.append(doi_id_norm)
			
			pmc_id = merged_pub.get('pmcid')
			if pmc_id is not None:
				curie_id = pub_common.pmcid2curie(pmc_id)
				initial_curie_ids.append(curie_id)
				pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
				putative_ids.append(pmc_id_norm)
			
			# Now use the first putative id
			merged_pub['id'] = putative_ids[0]
			
		return merged_pub
	
	def _mergeFoundPubsList(self,merging_list:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		"""
			This method takes an array of fetched entries, which could be merged
			in several ways, it groups the results, and returns the list of
			merged entries from those groups
		"""
		merged_results = []
		if merging_list:
			i2r = {}
			i2e = {}
			# Cluster by ids
			for merging_elem in merging_list:
				eId = None
				notSet = True
				
				pubmed_id = merging_elem.get('pmid')
				if pubmed_id is not None:
					if pubmed_id in i2r:
						eId = i2r[pubmed_id]
					else:
						eId = pubmed_id
						i2r[pubmed_id] = eId
					
					i2e.setdefault(eId,[]).append(merging_elem)
					notSet = False
				
				pmc_id = merging_elem.get('pmcid')
				if pmc_id is not None:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					if eId is None:
						if pmc_id_norm in i2r:
							eId = i2r[pmc_id_norm]
						else:
							eId = pmc_id_norm
					
					if pmc_id_norm not in i2r:
						i2r[pmc_id_norm] = eId
					
					# Avoid duplications
					if notSet:
						i2e.setdefault(eId,[]).append(merging_elem)
						notSet = False
				
				doi_id = merging_elem.get('doi')
				if doi_id is not None:
					doi_id_norm = pub_common.normalize_doi(doi_id)
					if eId is None:
						if doi_id_norm in i2r:
							eId = i2r[doi_id_norm]
						else:
							eId = doi_id_norm
					
					if doi_id_norm not in i2r:
						i2r[doi_id_norm] = eId
					
					# Avoid duplications
					if notSet:
						i2e.setdefault(eId,[]).append(merging_elem)
						notSet = False
			
			# After clustering the results, it is time to merge them
			# Duplicates should have been avoided (if possible)
			for found_pubs in i2e.values():
				merged_pub = self._mergeFoundPubs(found_pubs)
				merged_results.append(merged_pub)
		
		return merged_results
	
	def _transformIntoMergedPub(self,opeb_entry_pub):
		found_pubs = opeb_entry_pub['found_pubs']
		# We only need the reduced form for the next step
		merged_pub = self._mergeFoundPubs(found_pubs)
		# When no suitable entry is found, use the first one (if any)
		opeb_entry_pub['found_pubs'] = [ merged_pub ]  if merged_pub or not found_pubs else [ found_pubs[0] ]
		
		#print('-begin-',file=sys.stderr)
		#import json
		#print(json.dumps(opeb_entry_pub,indent=4),file=sys.stderr)
		#print('-end-',file=sys.stderr)
	
	def queryPubIdsBatch(self,query_ids:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		# Spawning the threads
		thread_pool = []
		for enricher_name, enricher  in self.enrichers.items():
			# We need the queue to put the returned results in some place
			et, eq = _thread_wrapper(enricher.cachedQueryPubIds,query_ids)
			thread_pool.append((et,eq,enricher_name))
		
		# Now, we gather the work of all threaded enrichers
		merging_list = []
		for et, eq, enricher_name  in thread_pool:
			# wait for it
			et.join()
			
			# The result (or the exception) is in the queue
			gathered_pairs = eq.get()
			
			# Kicking up the exception, so it is managed elsewhere
			if isinstance(gathered_pairs, BaseException):
				raise gathered_pairs
			
			# Labelling the results, so we know the enricher
			for gathered_pair in gathered_pairs:
				gathered_pair['enricher'] = enricher_name
			
			merging_list.extend(gathered_pairs)
		
		del thread_pool
		
		# and we process it
		merged_results = self._mergeFoundPubsList(merging_list)
		
		return merged_results
	
	def populatePubIdsBatch(self,partial_mappings:List[Dict[str,Any]]) -> None:
		if partial_mappings:
			raise Exception('FATAL ERROR: Cache miss. Should not happen')
	
		# Now, time to check and update cache
		partialToBeSearched = []
		i2e = {}
		for iPartial, partial_mapping in enumerate(partial_mappings):
			cached_mappings = self.pubC.getRawCachedMappingsFromPartial(partial_mapping)
			
			if cached_mappings:
				# Right now, we are not going to deal with conflicts at this level
				partial_mapping.update(cached_mappings[0])
			else:
				partialToBeSearched.append(partial_mapping)
				
				# Tracking where to place the results later
				pubmed_id = partial_mapping.get('pmid')
				if pubmed_id is not None:
					i2e.setdefault(pubmed_id,[]).append(iPartial)
				
				pmc_id = partial_mapping.get('pmcid')
				if pmc_id is not None:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					i2e.setdefault(pmc_id_norm,[]).append(iPartial)
				
				doi_id = partial_mapping.get('doi')
				if doi_id is not None:
					doi_id_norm = pub_common.normalize_doi(doi_id)
					i2e.setdefault(doi_id_norm,[]).append(iPartial)
		
		# Update cache with a new search
		if partialToBeSearched:
			rescuedPartials = self.cachedQueryPubIds(partialToBeSearched)
			for rescuedPartial in rescuedPartials:
				destPlaces = []
				
				pubmed_id = rescuedPartial.get('pmid')
				if pubmed_id is not None:
					destPlaces.extend(i2e.get(pubmed_id,[]))
				
				pmc_id = rescuedPartial.get('pmcid')
				if pmc_id is not None:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					destPlaces.extend(i2e.get(pmc_id_norm,[]))
				
				doi_id = rescuedPartial.get('doi')
				if doi_id is not None:
					doi_id_norm = pub_common.normalize_doi(doi_id)
					destPlaces.extend(i2e.get(doi_id_norm,[]))
				
				# Only the distinct ones
				if destPlaces:
					destPlaces = list(set(destPlaces))
					for destPlace in destPlaces:
						partial_mappings[destPlace] = rescuedPartial
	
	KEEP_KEYS=('source', 'id', 'year', 'enricher')
	def _cleanCitRefs(self,citrefs:List[Dict[str,Any]]) -> None:
		for citref in citrefs:
			for key in filter(lambda key: key not in self.KEEP_KEYS,citref.keys()):
				del citref[key]
	
	def listReconcileCitRefMetricsBatch(self,linear_pubs:List[Dict[str,Any]],verbosityLevel:float=0,mode:int=3) -> None:
		"""
			This method takes in batches of found publications and retrieves
			citations and / or references from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		
		# The publications are clustered by their original enricher
		clustered_pubs = {}
		for pub in linear_pubs:
			# This can happen when no result was found
			if 'base_pubs' in pub:
				for base_pub in pub['base_pubs']:
					if (base_pub.get('id') is not None and base_pub.get('source') is None) or (base_pub.get('id') is None and base_pub.get('source') is not None):
						print('FIXME',file=sys.stderr)
						print(base_pub,file=sys.stderr)
						sys.stderr.flush()
					
					clustered_pubs.setdefault(base_pub['enricher'],[]).append(base_pub)
		
		# After clustering, issue the batch calls to each threaded enricher
		thread_pool = []
		for enricher_name, base_pubs in clustered_pubs.items():
			enricher = self.enrichers[enricher_name]

			# for now, ignore the verbosity level, and use the one we need: 1.5
			et, eq = _thread_wrapper(enricher.listReconcileCitRefMetricsBatch,base_pubs,1.5,mode)
			thread_pool.append((et,eq,enricher_name))
		
		# Joining all the threads
		for et,eq,enricher_name in thread_pool:
			et.join()
			
			# The result (or the exception) is in the queue
			possible_exception = eq.get()
			
			# Kicking up the exception, so it is managed elsewhere
			if isinstance(possible_exception, BaseException):
				raise possible_exception
			
			# As the method enriches the results in place
		del thread_pool
		
		#print('-cbegin-',file=sys.stderr)
		#import json
		#print(json.dumps(clustered_pubs,indent=4),file=sys.stderr)
		#print('-cend-',file=sys.stderr)
		#sys.exit(1)
		
		# At last, reconcile!!!!!
		for merged_pub in linear_pubs:
			base_pubs = merged_pub.get('base_pubs',[])
			if base_pubs:
				toBeMergedRefs = []
				toBeMergedCits = []
				for base_pub in base_pubs:
					enricher = base_pub['enricher']
					# Labelling the citations
					if (mode & 2) != 0:
						cits = base_pub.get('citations')
						if cits:
							for cit in cits:
								cit['enricher'] = enricher
							toBeMergedCits.extend(cits)

					if (mode & 1) != 0:
						refs = base_pub.get('references')
						if refs:
							for ref in refs:
								ref['enricher'] = enricher
							toBeMergedRefs.extend(refs)
				
				# Any reference?
				if toBeMergedRefs:
					merged_temp_references = self._mergeFoundPubsList(toBeMergedRefs)
					
					#merged_references = self._mergeCitRef(map(lambda ref_pub: (ref_pub['enricher'],ref_pub.get('references')), base_pubs),verbosityLevel)
					
					# Now, time to check and update cache
					merged_references = []
					refsToBeSearched = []
					for ref in merged_temp_references:
						cached_refs = self.pubC.getRawCachedMappingsFromPartial(ref)
						
						if cached_refs:
							# Right now, we are not going to deal with conflicts at this level
							merged_references.append(cached_refs[0])
						else:
							refsToBeSearched.append(ref)
					
					# Update cache with a new search
					if refsToBeSearched:
						rescuedRefs = self.cachedQueryPubIds(refsToBeSearched)
						if rescuedRefs:
							merged_references.extend(rescuedRefs)
				
				# Any citation?
				if toBeMergedCits:
					merged_temp_citations = self._mergeFoundPubsList(toBeMergedCits)
					
					#merged_citations = self._mergeCitRef(map(lambda cit_pub: (cit_pub['enricher'],cit_pub.get('citations')), base_pubs),verbosityLevel)
					
					# Now, time to check and update cache
					merged_citations = []
					citsToBeSearched = []
					for cit in merged_temp_citations:
						cached_cits = self.pubC.getRawCachedMappingsFromPartial(cit)
						
						if cached_cits:
							# Right now, we are not going to deal with conflicts at this level
							merged_citations.append(cached_cits[0])
						else:
							citsToBeSearched.append(cit)
					
					# Update cache with a new search
					if citsToBeSearched:
						rescuedCits = self.cachedQueryPubIds(citsToBeSearched)
						if rescuedCits:
							merged_citations.extend(rescuedCits)
				
				# After merge, cleanup
				for base_pub in base_pubs:
					for key in 'references','reference_count','citations','citation_count':
						base_pub.pop(key,None)
			else:
				if (mode & 1) != 0:
					merged_references = merged_pub.get('references')
				if (mode & 2) != 0:
					merged_citations = merged_pub.get('citations')
			
			if (mode & 1) != 0:
				merged_pub['reference_count'] = 0  if merged_references is None else len(merged_references)
			if (mode & 2) != 0:
				merged_pub['citation_count'] = 0  if merged_citations is None else len(merged_citations)
			
			if verbosityLevel<=0:
				if (mode & 1) != 0:
					merged_pub['reference_stats'] = None  if merged_references is None else self._citrefStats(merged_references)
				if (mode & 2) != 0:
					merged_pub['citation_stats'] = None  if merged_citations is None else self._citrefStats(merged_citations)
			else:
				if (mode & 1) != 0:
					merged_pub['references'] = merged_references
				if (mode & 2) != 0:
					merged_pub['citations'] = merged_citations
				
				# Remove any key which is not the 'source', 'id', 'year' or 'enricher'
				if verbosityLevel==1:
					if (merged_references is not None) and ((mode & 1) != 0):
						self._cleanCitRefs(merged_references)
					if (merged_citations is not None) and ((mode & 2) != 0):
						self._cleanCitRefs(merged_citations)
				elif (merged_citations is not None) and (verbosityLevel >=2):
					self.listReconcileCitRefMetricsBatch(merged_citations,verbosityLevel-1,mode)


# This is needed for the program itself
DEFAULT_BACKEND = EuropePMCEnricher
RECOGNIZED_BACKENDS_HASH = OrderedDict( MetaEnricher.RECOGNIZED_BACKENDS_HASH )
RECOGNIZED_BACKENDS_HASH[ MetaEnricher.Name() ] = MetaEnricher
