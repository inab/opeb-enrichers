#!/usr/bin/python

import os
import datetime
import shelve
from typing import Tuple, List, Dict, Any, NewType

from . import pub_common
from .pub_common import Timestamps

# Alias types declaration
Citation = NewType('Citation',Dict[str,Any])
CitationCount = NewType('CitationCount',int)
Reference = NewType('Reference',Dict[str,Any])
ReferenceCount = NewType('ReferenceCount',int)
Mapping = NewType('Mapping',Dict[str,Any])
UnqualifiedId = NewType('UnqualifiedId',str)
SourceId = NewType('SourceId',str)
QualifiedId = NewType('QualifiedId',Tuple[SourceId,UnqualifiedId])
PublishId = NewType('PublishId',str)

class PubCache(object):
	"""
		The publications cache management code
		Currently, it stores the correspondence among PMIDs,
		PMC ids, DOIs and the internal identifier in the
		original source.
		Also, it stores the title, etc..
		Also, it stores the citations fetched from the original source
	"""
	DEFAULT_CACHE_CITATIONS_FILE="pubEnricherCits.shelve"
	DEFAULT_CACHE_REFERENCES_FILE="pubEnricherRefs.shelve"
	DEFAULT_CACHE_PUB_IDS_FILE="pubEnricherIds.shelve"
	DEFAULT_CACHE_PUB_IDMAPS_FILE="pubEnricherIdMaps.shelve"
	
	OLDEST_CACHE = datetime.timedelta(days=28)

	def __init__(self,cache_dir:str=".",prefix:str=None):
		self.cache_dir = cache_dir
		
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		# Should we set a prefix for the shelves?
		if prefix is None:
			cache_citations_file = self.DEFAULT_CACHE_CITATIONS_FILE
			cache_references_file = self.DEFAULT_CACHE_REFERENCES_FILE
			cache_pub_ids_file = self.DEFAULT_CACHE_PUB_IDS_FILE
			cache_pub_idmaps_file = self.DEFAULT_CACHE_PUB_IDMAPS_FILE
		else:
			cache_citations_file = prefix + self.DEFAULT_CACHE_CITATIONS_FILE
			cache_references_file = prefix + self.DEFAULT_CACHE_REFERENCES_FILE
			cache_pub_ids_file = prefix + self.DEFAULT_CACHE_PUB_IDS_FILE
			cache_pub_idmaps_file = prefix + self.DEFAULT_CACHE_PUB_IDMAPS_FILE
		
		self.cache_citations_file = os.path.join(cache_dir,cache_citations_file)
		self.cache_references_file = os.path.join(cache_dir,cache_references_file)
		self.cache_ids_file = os.path.join(cache_dir,cache_pub_ids_file)
		self.cache_idmaps_file = os.path.join(cache_dir,cache_pub_idmaps_file)
	
	def __enter__(self):
		self.cache_citations = shelve.open(self.cache_citations_file)
		self.cache_references = shelve.open(self.cache_references_file)
		self.cache_ids = shelve.open(self.cache_ids_file)
		self.cache_idmaps = shelve.open(self.cache_idmaps_file)
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb) -> None:
		self.cache_citations.close()
		self.cache_references.close()
		self.cache_ids.close()
		self.cache_idmaps.close()
	
	
	def sync(self) -> None:
		self.cache_citations.sync()
		self.cache_references.sync()
		self.cache_ids.sync()
		self.cache_idmaps.sync()
	
	@staticmethod
	def _getRefId(source_id:SourceId,_id:UnqualifiedId) -> str:
		return source_id + ':' + _id
	
	def getCitationsAndCount(self,source_id:SourceId,_id:UnqualifiedId) -> Tuple[List[Citation],CitationCount]:
		refId = PubCache._getRefId(source_id,_id)
		citations_timestamp , citations , citation_count = self.cache_citations.get(refId,(None,None,None))
		
		# Invalidate cache
		if citations_timestamp is not None and (Timestamps.UTCTimestamp() - citations_timestamp) > self.OLDEST_CACHE:
			citations = None
			citation_count = None
		
		return citations,citation_count
	
	def setCitationsAndCount(self,source_id:SourceId,_id:UnqualifiedId,citations:List[Citation],citation_count:CitationCount,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		refId = PubCache._getRefId(source_id,_id)
		self.cache_citations[refId] = (timestamp,citations,citation_count)
	
	def getReferencesAndCount(self,source_id:SourceId,_id:UnqualifiedId) -> Tuple[List[Reference],ReferenceCount]:
		refId = PubCache._getRefId(source_id,_id)
		references_timestamp , references , reference_count = self.cache_references.get(refId,(None,None,None))
		
		# Invalidate cache
		if references_timestamp is not None and (Timestamps.UTCTimestamp() - references_timestamp) > self.OLDEST_CACHE:
			references = None
			reference_count = None
		
		return references,reference_count
	
	def setReferencesAndCount(self,source_id:SourceId,_id:UnqualifiedId,references:List[Reference],reference_count:ReferenceCount,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		refId = PubCache._getRefId(source_id,_id)
		self.cache_references[refId] = (timestamp,references,reference_count)
		
	def getRawCachedMapping(self,source_id:SourceId,_id:UnqualifiedId) -> Mapping:
		"""
			This method does not invalidate the cache
		"""
		
		refId = PubCache._getRefId(source_id,_id)
		mapping_timestamp , mapping = self.cache_idmaps.get(refId,(None,None))
		return mapping_timestamp , mapping
	
	def getCachedMapping(self,source_id:SourceId,_id:UnqualifiedId) -> Mapping:
		mapping_timestamp , mapping = self.getRawCachedMapping(source_id,_id)
		
		# Invalidate cache
		if mapping_timestamp is not None and (Timestamps.UTCTimestamp() - mapping_timestamp) > self.OLDEST_CACHE:
			mapping = None
		
		return mapping
	
	def setCachedMapping(self,mapping:Mapping,mapping_timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		_id = mapping['id']
		source_id = mapping['source']
		
		# Fetching previous version
		refId = PubCache._getRefId(source_id,_id)
		old_mapping_timestamp , old_mapping = self.getRawCachedMapping(source_id,_id)
		
		# First, store
		self.cache_idmaps[refId] = (mapping_timestamp,mapping)
		
		# Then, cleanup of sourceIds cache
		pubmed_id = mapping.get('pmid')
		pmc_id = mapping.get('pmcid')
		pmc_id_norm = pub_common.normalize_pmcid(pmc_id)  if pmc_id else None
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
		old_pmc_id_norm = pub_common.normalize_pmcid(old_pmc_id)  if old_pmc_id else None
		
		for old_id, new_id in [(old_pubmed_id,pubmed_id),(old_doi_id_norm,doi_id_norm),(old_pmc_id_norm,pmc_id)]:
			# Code needed for mismatches
			if old_id is not None and old_id != new_id:
				self.removeSourceId(old_id,source_id,_id,timestamp=mapping_timestamp)
			
			if new_id is not None and old_id != new_id:
				self.appendSourceId(new_id,source_id,_id,timestamp=mapping_timestamp)
	
	def getRawSourceIds(self,publish_id:PublishId) -> List[QualifiedId]:
		"""
			This method does not invalidate the cache
		"""
		timestamp_internal_ids , internal_ids = self.cache_ids.get(publish_id,(None,[]))
		return timestamp_internal_ids , internal_ids
	
	def getSourceIds(self,publish_id:PublishId) -> List[QualifiedId]:
		timestamp_internal_ids , internal_ids = self.getRawSourceIds(publish_id)
		
		# Invalidate cache
		if timestamp_internal_ids is None or (Timestamps.UTCTimestamp() - timestamp_internal_ids) > self.OLDEST_CACHE:
			internal_ids = None
		
		return internal_ids
	
	def appendSourceId(self,publish_id:PublishId,source_id:SourceId,_id:UnqualifiedId,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		timestamp_internal_ids , internal_ids = self.getRawSourceIds(publish_id)

		# Invalidate cache
		if timestamp_internal_ids is not None and (Timestamps.UTCTimestamp() - timestamp_internal_ids) > self.OLDEST_CACHE:
			internal_ids = []
		
		internal_ids.append((source_id,_id))

		self.cache_ids[publish_id] = (timestamp,internal_ids)
	
	def removeSourceId(self,publish_id:PublishId,source_id:SourceId,_id:UnqualifiedId,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		orig_timestamp , internal_ids = self.getRawSourceIds(publish_id)
		
		if orig_timestamp is not None:
			try:
				# Invalidate cache
				if (Timestamps.UTCTimestamp() - orig_timestamp) > self.OLDEST_CACHE:
					internal_ids = []
				else:
					internal_ids.remove((source_id,_id))
				self.cache_ids[publish_id] = (timestamp,internal_ids)
			except:
				pass
	
	def getRawCachedMappingsFromPartial(self,partial_mapping:Mapping) -> Mapping:
		"""
			This method returns one or more cached mappings, based on the partial
			mapping provided. First attempt is using the id, otherwise it derives
			on the pmid, pmcid and doi to rescue, if available.
			
			This method does not invalidate caches
		"""
		mappings = []
		if partial_mapping.get('id'):
			_ , mapping = self.getRawCachedMapping(partial_mapping.get('source'),partial_mapping.get('id'))
			if mapping:
				mappings.append(mapping)
		
		if not mappings and (partial_mapping.get('pmid') or partial_mapping.get('pmcid') or partial_mapping.get('doi')):
			mapping_ids = []
			for field_name in ('pmid','pmcid','doi'):
				_theId = partial_mapping.get(field_name)
				if _theId:
					_ , internal_ids = self.getRawSourceIds(_theId)
					# Only return when internal_ids is a
					if internal_ids:
						mapping_ids.extend(internal_ids)
			
			if mapping_ids:
				# Trying to avoid duplicates
				mapping_ids = list(set(mapping_ids))
				mappings.extend(filter(lambda r: r is not None, map(lambda _iId: self.getRawCachedMapping(*_iId)[1], mapping_ids)))
		
		return mappings


EnricherId = NewType('EnricherId',str)
MetaQualifiedId = NewType('MetaQualifiedId',Tuple[EnricherId,SourceId,UnqualifiedId])

class MetaPubCache(PubCache):
	DEFAULT_CACHE_LOWER_MAPPING_FILE="pubEnricherLower.shelve"
	
	def __init__(self,cache_dir:str=".",prefix:str=None):
		super().__init__(cache_dir,prefix)
		
		# Should we set a prefix for the metashelves?
		if prefix is None:
			cache_lower_mapping_file = self.DEFAULT_CACHE_LOWER_MAPPING_FILE
		else:
			cache_lower_mapping_file = prefix + self.DEFAULT_CACHE_LOWER_MAPPING_FILE
		
		self.cache_lower_mapping_file = os.path.join(cache_dir,cache_lower_mapping_file)
	
	def __enter__(self):
		super().__enter__()
		self.cache_lower_mapping = shelve.open(self.cache_lower_mapping_file)
		
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb) -> None:
		self.cache_lower_mapping.close()
		super().__exit__(exc_type,exc_val,exc_tb)
	
	
	def sync(self) -> None:
		self.cache_lower_mapping.sync()
		super().sync()
	
	def setCachedMapping(self,mapping:Mapping,mapping_timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		# Before anything, get the previous mapping before updating it
		_id = mapping['id']
		source_id = mapping['source']
		
		# Fetching previous version
		old_mapping_timestamp , old_mapping = self.getRawCachedMapping(source_id,_id)
		
		# Storing the mapping
		super().setCachedMapping(mapping,mapping_timestamp)
		
		# Let's manage also the lower mappings, from base_pubs
		
		# Creating the sets
		oldLowerSet = set()
		if old_mapping:
			old_base_pubs = old_mapping.get('base_pubs',[])
			for old_lower in old_base_pubs:
				if old_lower.get('id'):
					oldLowerSet.add((old_lower['enricher'],old_lower['source'],old_lower['id']))
		
		newLowerSet = set()
		new_base_pubs = mapping.get('base_pubs',[])
		for new_lower in new_base_pubs:
			if new_lower.get('id'):
				newLowerSet.add((new_lower['enricher'],new_lower['source'],new_lower['id']))
		
		# This set has the entries to be removed
		toRemoveSet = oldLowerSet - newLowerSet
		for removeFromLower in toRemoveSet:
			self.removeMetaSourceId(removeFromLower,source_id,_id,mapping_timestamp)
		
		# This set has the entries to be added
		toAddSet = newLowerSet - oldLowerSet
		for addToLower in toAddSet:
			self.appendMetaSourceId(addToLower,source_id,_id,mapping_timestamp)
	
	@staticmethod
	def _getLowerRefId(lower:MetaQualifiedId) -> str:
		return lower[2] + ':' + lower[1] + ':' + lower[0]
	
	def getRawMetaSourceIds(self,lower:MetaQualifiedId) -> List[QualifiedId]:
		"""
			This method does not invalidate caches
		"""
		lower_enricher, lower_source, lower_id = lower
		lowerRefId = MetaPubCache._getLowerRefId(lower)
		timestamp_meta_ids , meta_ids = self.cache_lower_mapping.get(lowerRefId,(None,[]))
		
		return timestamp_meta_ids, meta_ids
	
	def getMetaSourceIds(self,lower:MetaQualifiedId) -> List[QualifiedId]:
		timestamp_meta_ids , meta_ids = self.getRawMetaSourceIds(lower)
		
		# Invalidate cache
		if timestamp_meta_ids is None or (Timestamps.UTCTimestamp() - timestamp_meta_ids) > self.OLDEST_CACHE:
			meta_ids = None
		
		return meta_ids
	
	def appendMetaSourceId(self,lower:MetaQualifiedId,source_id:SourceId,_id:UnqualifiedId,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		timestamp_meta_ids , meta_ids = self.getRawMetaSourceIds(lower)

		# Invalidate cache
		if timestamp_meta_ids is not None and (Timestamps.UTCTimestamp() - timestamp_meta_ids) > self.OLDEST_CACHE:
			meta_ids = []
		
		meta_ids.append((source_id,_id))

		lowerRefId = MetaPubCache._getLowerRefId(lower)
		self.cache_lower_mapping[lowerRefId] = (timestamp,meta_ids)
	
	def removeMetaSourceId(self,lower:MetaQualifiedId,source_id:SourceId,_id:UnqualifiedId,timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		orig_timestamp , meta_ids = self.getRawMetaSourceIds(lower)
		
		if orig_timestamp is not None:
			try:
				# Invalidate cache
				if (Timestamps.UTCTimestamp() - orig_timestamp) > self.OLDEST_CACHE:
					meta_ids = []
				else:
					meta_ids.remove((source_id,_id))
				lowerRefId = MetaPubCache._getLowerRefId(lower)
				self.cache_lower_mapping[lowerRefId] = (timestamp,meta_ids)
			except:
				pass
	
	def getRawCachedMappingsFromPartial(self,partial_mapping:Mapping) -> List[Mapping]:
		"""
			This method returns one or more cached mappings, based on the partial
			mapping provided. First attempt is using the id, then it tries through
			base_pubs information. Last, it derives	on the pmid, pmcid and doi to
			rescue, if available.
			
			This method does not invalidate caches
		"""
		mappings = []
		mapping_ids = []
		if partial_mapping.get('id'):
			_ , mapping = self.getRawCachedMapping(partial_mapping.get('source'),partial_mapping.get('id'))
			if mapping:
				mappings.append(mapping)
		
		# Now, trying with the identifiers of the mapped publications
		if not mappings:
			base_pubs = partial_mapping.get('base_pubs',[])
			if base_pubs:
				for base_pub in base_pubs:
					_, internal_ids = self.getRawMetaSourceIds((base_pub.get('enricher'),base_pub.get('source'),base_pub.get('id')))
					if internal_ids:
						mapping_ids.extend(internal_ids)
		
		# Last resort
		if not mappings and (partial_mapping.get('pmid') or partial_mapping.get('pmcid') or partial_mapping.get('doi')):
			for field_name in ('pmid','pmcid','doi'):
				_theId = partial_mapping.get(field_name)
				if _theId:
					_ , internal_ids = self.getRawSourceIds(_theId)
					# Only return when internal_ids is a
					if internal_ids:
						mapping_ids.extend(internal_ids)
		
		if mapping_ids:
			# Trying to avoid duplicates
			mapping_ids = list(set(mapping_ids))
			mappings.extend(filter(lambda r: r is not None, map(lambda _iId: self.getRawCachedMapping(*_iId)[1], mapping_ids)))
		
		return mappings
