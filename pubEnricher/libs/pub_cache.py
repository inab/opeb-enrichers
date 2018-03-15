#!/usr/bin/python

import os
import datetime
import shelve
from typing import Tuple, List, Dict, Any

from . import pub_common
from .pub_common import Timestamps

class PubCache:
	"""
		The publications cache management code
		Currently, it stores the correspondence among PMIDs,
		PMC ids, DOIs and the internal identifier in the
		original source.
		Also, it stores the title, etc..
		Also, it stores the citations fetched from the original source
	"""
	DEFAULT_CACHE_CITATIONS_FILE="pubEnricherCits.shelve"
	DEFAULT_CACHE_PUB_IDS_FILE="pubEnricherIds.shelve"
	DEFAULT_CACHE_PUB_IDMAPS_FILE="pubEnricherIdMaps.shelve"
	
	OLDEST_CACHE = datetime.timedelta(days=28)

	def __init__(self,cache_dir:str="."):
		self.cache_dir = cache_dir
		
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		self.cache_citations_file = os.path.join(cache_dir,self.DEFAULT_CACHE_CITATIONS_FILE)
		self.cache_ids_file = os.path.join(cache_dir,self.DEFAULT_CACHE_PUB_IDS_FILE)
		self.cache_idmaps_file = os.path.join(cache_dir,self.DEFAULT_CACHE_PUB_IDMAPS_FILE)
	
	def __enter__(self):
		self.cache_citations = shelve.open(self.cache_citations_file)
		self.cache_ids = shelve.open(self.cache_ids_file)
		self.cache_idmaps = shelve.open(self.cache_idmaps_file)
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb) -> None:
		self.cache_citations.close()
		self.cache_ids.close()
		self.cache_idmaps.close()
	
	
	def sync(self) -> None:
		self.cache_citations.sync()
		self.cache_ids.sync()
		self.cache_idmaps.sync()
	
	def getCitationsAndCount(self,source_id:str,_id:str) -> Tuple[List[Dict[str,Any]],int]:
		refId = source_id+':'+_id
		citations_timestamp , citations , citations_count = self.cache_citations.get(refId,(None,None,None))
		
		# Invalidate cache
		if citations_timestamp is not None and (Timestamps.UTCTimestamp() - citations_timestamp) > self.OLDEST_CACHE:
			citations = None
			citations_count = None
		
		return citations,citations_count
	
	def setCitationsAndCount(self,source_id:str,_id:str,citations:List[Dict[str,Any]],citations_count:int,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		refId = source_id+':'+_id
		self.cache_citations[refId] = (Timestamps.UTCTimestamp(),citations,citations_count)
	
	def getRawCachedMapping(self,source_id:str,_id:str) -> Dict[str,Any]:
		refId = source_id+':'+_id
		mapping_timestamp , mapping = self.cache_idmaps.get(refId,(None,None))
		return mapping_timestamp , mapping
	
	def getCachedMapping(self,source_id:str,_id:str) -> Dict[str,Any]:
		mapping_timestamp , mapping = self.getRawCachedMapping(source_id,_id)
		
		# Invalidate cache
		if mapping_timestamp is not None and (Timestamps.UTCTimestamp() - mapping_timestamp) > self.OLDEST_CACHE:
			mapping = None
		
		return mapping
	
	def setCachedMapping(self,mapping:Dict[str,Any],mapping_timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		_id = mapping['id']
		source_id = mapping['source']
		
		# Fetching previous version
		refId = source_id+':'+_id
		old_mapping_timestamp , old_mapping = self.getRawCachedMapping(source_id,_id)
		
		# First, store
		self.cache_idmaps[refId] = (mapping_timestamp,mapping)
		
		# Then, cleanup of sourceIds cache
		pubmed_id = mapping.get('pmid')
		pmc_id = mapping.get('pmcid')
		doi_id = mapping.get('doi')
		doi_id_norm = pub_common.normalize_doi(doi_id)  if doi_id else None
		
		if old_mapping_timestamp is not None:
			old_pubmed_id = old_mapping.get('pmid')
			old_doi_id = old_mapping.get('doi')
			old_pmc_id = old_mapping.get('pmcid')
		else:
			old_pubmed_id = None
			old_doi_id = None
			old_pmc_id = None
		old_doi_id_norm = pub_common.normalize_doi(old_doi_id)  if old_doi_id else None
		
		for old_id, new_id in [(old_pubmed_id,pubmed_id),(old_doi_id_norm,doi_id_norm),(old_pmc_id,pmc_id)]:
			# Code needed for mismatches
			if old_id is not None and old_id != new_id:
				self.removeSourceId(old_id,source_id,_id,timestamp=mapping_timestamp)
			
			if new_id is not None and old_id != new_id:
				self.appendSourceId(new_id,source_id,_id,timestamp=mapping_timestamp)
	
	def getSourceIds(self,publish_id:str) -> List[str]:
		timestamp_internal_ids , internal_ids = self.cache_ids.get(publish_id,(None,None))
		return internal_ids
	
	def appendSourceId(self,publish_id:str,source_id:str,_id:str,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		_ , internal_ids = self.cache_ids.get(publish_id,(None,[]))
		internal_ids.append((source_id,_id))
		self.cache_ids[publish_id] = (timestamp,internal_ids)
	
	def removeSourceId(self,publish_id:str,source_id:str,_id:str,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		orig_timestamp , internal_ids = self.cache_ids.get(publish_id,(None,[]))
		
		if orig_timestamp is not None:
			try:
				internal_ids.remove((source_id,_id))
				self.cache_ids[publish_id] = (timestamp,internal_ids)
			except:
				pass
