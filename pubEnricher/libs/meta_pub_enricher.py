#!/usr/bin/python

import sys
import json
import re
import configparser

from collections import OrderedDict
import copy

#from multiprocessing import Process, Queue, Lock

from typing import overload, Tuple, List, Dict, Any, Iterator

from .pub_cache import PubCache
from .skeleton_pub_enricher import SkeletonPubEnricher
from .europepmc_enricher import EuropePMCEnricher
from .pubmed_enricher import PubmedEnricher
from .wikidata_enricher import WikidataEnricher

from . import pub_common

class MetaEnricher(SkeletonPubEnricher):
	RECOGNIZED_BACKENDS = [ EuropePMCEnricher, PubmedEnricher, WikidataEnricher ]
	RECOGNIZED_BACKENDS_HASH = OrderedDict( ( (backend.Name(),backend) for backend in RECOGNIZED_BACKENDS ) )
	
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
		
		super().__init__(cache,prefix,config,debug)
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		# Create the instances needed by this meta enricher
		use_enrichers_str = self.config.get(section_name,'use_enrichers',fallback=None)
		
		use_enrichers = use_enrichers_str.split(',')  if use_enrichers_str else self.RECOGNIZED_BACKENDS_HASH.keys()
		
		self.enrichers = OrderedDict()
		for enricher_name in use_enrichers:
			enricher_class = self.RECOGNIZED_BACKENDS_HASH.get(enricher_name)
			if enricher_class:
				compound_prefix = prefix + enricher_name if prefix else enricher_name
				# Each value is an instance of AbstractPubEnricher
				self.enrichers[enricher_name] = enricher_class(cache,compound_prefix,config,debug)
	
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
	def _initializeMergedEntry(self,entry:Dict[str,Any],entry_enricher:str,base_entry:Dict[str,Any]=None) -> Dict[str,Any]:
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
		merged_pub = None
		if found_pubs:
			base_pubs = found_pubs
			for i_found_pub,base_found_pub in enumerate(found_pubs):
				if base_found_pub['id'] is not None:
					merged_pub = copy.deepcopy(base_found_pub)
					for found_pub in found_pubs[i_found_pub+1:]:
						if found_pub['id'] is not None:
							for key,val in found_pub.items():
								merged_pub.setdefault(key,val)
					
					#merged_pub['base_pubs'] = base_pubs
					# We only need the reduced form for the next step
					merged_pub['base_pubs'] = [ {'id': pub['id'],'source':pub['source'],'enricher':pub['enricher']}  for pub in found_pubs ]
					merged_pub['source'] = self.META_SOURCE
					merged_pub['id'] =  '-'.join(map(lambda pub: pub['enricher']+':'+pub['source']+':'+pub['id'] , filter(lambda pub: pub['id'] is not None , merged_pub['base_pubs'])))
					del merged_pub['enricher']
					
					break
		
		return merged_pub
	
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
	
	def reconcilePubIdsBatch(self,opeb_entries:List[Any]) -> None:
		# We need to save the original state before any call
		pristine_opeb_entries = copy.deepcopy(opeb_entries)
		initialized = False
		for enricher_name, enricher  in self.enrichers.items():
			enriched_entries = copy.deepcopy(pristine_opeb_entries)  if initialized else opeb_entries
			
			enricher.reconcilePubIdsBatch(enriched_entries)
				
			if initialized:
				for base_entry_pubs, entry_pubs in zip(map(lambda entry: entry.get('entry_pubs',[]),opeb_entries),map(lambda entry: entry.get('entry_pubs',[]),enriched_entries)):
					for base_entry,entry in zip(base_entry_pubs,entry_pubs):
						# The base_entry receives this merged entry
						self._initializeMergedEntry(entry,enricher_name,base_entry)
			else:
				initialized = True
				# Redoing the results to something understandable
				for opeb_entry_pubs in map(lambda opeb_entry: opeb_entry['entry_pubs'],opeb_entries):
					initialized_entry_pubs = [ self._initializeMergedEntry(entry_pub,enricher_name)  for entry_pub in opeb_entry_pubs]
					opeb_entry_pubs.clear()
					opeb_entry_pubs.extend(initialized_entry_pubs)
			
		
		# Now, merge!!!!
		for opeb_entry_pubs in map(lambda opeb_entry: opeb_entry['entry_pubs'],opeb_entries):
			for opeb_entry_pub in opeb_entry_pubs:
				self._transformIntoMergedPub(opeb_entry_pub)
		#print('-obegin-',file=sys.stderr)
		#import json
		#print(json.dumps(opeb_entry_pubs,indent=4),file=sys.stderr)
		#print('-oend-',file=sys.stderr)
		#sys.exit(1)
	
	# It merges one or more citations of reference lists
	def _mergeCitRef(self,citrefListSet:Iterator[Tuple[str,List[Dict[str,Any]]]],verbosityLevel:float) -> List[Dict[str,Any]]:
		merged_citrefList = []
		# The inverse correspondence
		pubmed2citref = {}
		doi2citref = {}
		pmcid2citref = {}
		
		for enricher_name,citrefList in citrefListSet:
			#print('-mbegin {0}-'.format(enricher_name),file=sys.stderr)
			#import json
			#print(json.dumps(citrefList,indent=4),file=sys.stderr)
			#print('-mend-',file=sys.stderr)
			#sys.exit(1)
			for citref in citrefList:
				merged_citref = None
				
				# First, gather what we already know
				pubmed_id = citref.get('pmid')
				if pubmed_id is not None:
					merged_citref = pubmed2citref.get(pubmed_id)
				
				doi_id = citref.get('doi')
				if doi_id:
					doi_id_norm = pub_common.normalize_doi(doi_id)
					if merged_citref is None:
						merged_citref = doi2citref.get(doi_id_norm)
				else:
					doi_id_norm = None
				
				pmc_id = citref.get('pmcid')
				if pmc_id:
					pmc_id_norm = pub_common.normalize_pmcid(pmc_id)
					if merged_citref is None:
						merged_citref = pmcid2citref.get(pmc_id_norm)
				else:
					pmc_id_norm = None
				
				# Then, either add or merge
				if merged_citref is None:
					merged_citref = self._initializeMergedEntry({'found_pubs':[citref]},enricher_name)
					#print('-imebegin-',file=sys.stderr)
					#import json
					#print(json.dumps(merged_citref,indent=4),file=sys.stderr)
					#print('-imeend-',file=sys.stderr)
					#sys.exit(1)
					merged_citrefList.append(merged_citref)
				
				else:
					# Merging the info
					self._initializeMergedEntry({'found_pubs':[citref]},enricher_name,merged_citref)
					#print('-mmebegin-',file=sys.stderr)
					#import json
					#print(json.dumps(citref,indent=4),file=sys.stderr)
					#print(json.dumps(merged_citref,indent=4),file=sys.stderr)
					#print('-mmeend-',file=sys.stderr)
					#sys.exit(1)
				
				# Updating internal, in memory citref cache
				if pubmed_id:
					pubmed2citref.setdefault(pubmed_id, merged_citref)
				if doi_id_norm:
					doi2citref.setdefault(doi_id_norm, merged_citref)
				if pmc_id_norm:
					pmcid2citref.setdefault(pmc_id_norm, merged_citref)
		
		#print('-mebegin-',file=sys.stderr)
		#import json
		#print(json.dumps(merged_citrefList,indent=4),file=sys.stderr)
		#print('-meend-',file=sys.stderr)
		#sys.exit(1)
		
		# Now, resolve it!
		resolved_citrefs = []
		for merged_citref in merged_citrefList:
			found_citrefs = merged_citref['found_pubs']
			merged_pub = self._mergeFoundPubs(found_citrefs)
			if merged_pub:
				resolved_citrefs.append(merged_pub)
			else:
				resolved_citrefs.extend(found_citrefs)
		
		#print('-begin-',file=sys.stderr)
		#import json
		#print(json.dumps(resolved_citrefs,indent=4),file=sys.stderr)
		#print('-end-',file=sys.stderr)
		return resolved_citrefs
	
	KEEP_KEYS=('source', 'id', 'year', 'enricher')
	def _cleanCitRefs(self,citrefs:List[Dict[str,Any]]) -> None:
		for citref in citrefs:
			for key in filter(lambda key: key not in self.KEEP_KEYS,citref.keys()):
				del citref[key]
	
	def listReconcileCitRefMetricsBatch(self,linear_pubs:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of found publications and retrieves citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		clustered_pubs = {}
		for pub in linear_pubs:
			# This can happen when no result was found
			if 'base_pubs' in pub:
				for base_pub in pub['base_pubs']:
					clustered_pubs.setdefault(base_pub['enricher'],[]).append(base_pub)
		
		# After clustering, issue the batch calls to each enricher
		for enricher_name, base_pubs in clustered_pubs.items():
			enricher = self.enrichers[enricher_name]
			# for now, ignore the verbosity level, and use the one we need: 1.5
			enricher.listReconcileCitRefMetricsBatch(base_pubs,1.5)
		
		#print('-cbegin-',file=sys.stderr)
		#import json
		#print(json.dumps(clustered_pubs,indent=4),file=sys.stderr)
		#print('-cend-',file=sys.stderr)
		#sys.exit(1)
		
		# At last, reconcile!!!!!
		for merged_pub in linear_pubs:
			base_pubs = merged_pub.get('base_pubs',[])
			if base_pubs:
				merged_references = self._mergeCitRef(map(lambda ref_pub: (ref_pub['enricher'],ref_pub['references']), filter(lambda ref_pub: ref_pub.get('references') is not None , base_pubs)),verbosityLevel)
				merged_citations = self._mergeCitRef(map(lambda cit_pub: (cit_pub['enricher'],cit_pub['citations']), filter(lambda cit_pub: cit_pub.get('citations') is not None , base_pubs)),verbosityLevel)
				
				# Cleanup
				for base_pub in base_pubs:
					for key in 'references','reference_count','citations','citation_count':
						base_pub.pop(key,None)
			else:
				merged_references = merged_pub.get('references',[])
				merged_citations = merged_pub.get('citations',[])
			
			merged_pub['reference_count'] = len(merged_references)
			merged_pub['citation_count'] = len(merged_citations)
			
			if verbosityLevel<=0:
				merged_pub['citation_stats'] = self._citrefStats(merged_citations)
				merged_pub['reference_stats'] = self._citrefStats(merged_references)
			else:
				merged_pub['references'] = merged_references
				merged_pub['citations'] = merged_citations
				
				# Remove any key which is not the 'source', 'id', 'year' or 'enricher'
				if verbosityLevel==1:
					self._cleanCitRefs(merged_references)
					self._cleanCitRefs(merged_citations)
				elif verbosityLevel >=2:
					self.listReconcileCitRefMetricsBatch(merged_citations,verbosityLevel-1)


# This is needed for the program itself
DEFAULT_BACKEND = EuropePMCEnricher
RECOGNIZED_BACKENDS_HASH = OrderedDict( MetaEnricher.RECOGNIZED_BACKENDS_HASH )
RECOGNIZED_BACKENDS_HASH[ MetaEnricher.Name() ] = MetaEnricher
