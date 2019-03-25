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
	
	@classmethod
	@abstractmethod
	def Name(cls) -> str:
		return 'abstract'
	
	@abstractmethod
	def queryCitRefsBatch(self,query_citations_data:Iterator[Dict[str,Any]],minimal:bool=False,mode:int=3) -> Iterator[Dict[str,Any]]:
		"""
		query_citations_data: An iterator of dictionaries with at least two keys: source and id
		minimal: Whether the list of citations and references is "minimal" (minimizing the number of queries) or not
		mode: 1 means only references, 2 means only citations, and 3 means both
		"""
		pass
	
	
	def searchClusteredCitRefMetricsBatch(self,query_citations_data:List[Dict[str,Any]],query_hash,minimal:bool=False,mode:int=3) -> None:
		# Update the cache with the new data
		if len(query_citations_data) > 0:
			try:
				new_citations = self.queryCitRefsBatch(query_citations_data,minimal,mode)
			except Exception as anyEx:
				print("ERROR: Something went wrong",file=sys.stderr)
				print(anyEx,file=sys.stderr)
				raise anyEx
			
			for new_citation in new_citations:
				source_id = new_citation['source']
				_id = new_citation['id']
				
				if (mode & 2) != 0:
					if 'citations' in new_citation:
						citations = new_citation['citations']
						citation_count = new_citation['citation_count']
						# There are cases where no citation could be fetched
						# but it should also be cached
						self.pubC.setCitationsAndCount(source_id,_id,citations,citation_count)
						for pub_field in query_hash[(_id,source_id)]:
							pub_field['citation_count'] = citation_count
							pub_field['citations'] = citations
				
				if (mode & 1) != 0:
					if 'references' in new_citation:
						references = new_citation['references']
						reference_count = new_citation['reference_count']
						# There are cases where no reference could be fetched
						# but it should also be cached
						self.pubC.setReferencesAndCount(source_id,_id,references,reference_count)
						for pub_field in query_hash[(_id,source_id)]:
							pub_field['reference_count'] = reference_count
							pub_field['references'] = references
	
	def listReconcileCitRefMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0,mode:int=3) -> None:
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
				
				if ( mode & 2 ) != 0:
					citations, citation_count = self.pubC.getCitationsAndCount(source_id,_id)
					if citation_count is not None:
						# Save now
						pub_field['citation_count'] = citation_count
						pub_field['citations'] = citations

				if ( mode & 1 ) != 0:
					references, reference_count = self.pubC.getReferencesAndCount(source_id,_id)
					if reference_count is not None:
						# Save now
						pub_field['reference_count'] = reference_count
						pub_field['references'] = references
				
				# Query later, without repetitions
				if ((mode & 2) != 0 and (citation_count is None)) or ((mode & 1) != 0 and (reference_count is None)):
					query_list = query_hash.setdefault((_id,source_id),[])
					if len(query_list) == 0:
						query_citations_data.append(pub_field)
					query_list.append(pub_field)
		
		minimal = verbosityLevel == -1
		# Update the cache with the new data
		self.searchClusteredCitRefMetricsBatch(query_citations_data,query_hash,minimal,mode)
		
		# If we have to return the digested stats, compute them here
		if verbosityLevel > -1 and verbosityLevel<=0:
			for pub_field in pub_list:
				if (mode & 2) != 0:
					citations = pub_field.pop('citations',None)
					# Computing the stats
					pub_field['citation_stats'] = None  if citations is None else self._citrefStats(citations)
				
				if (mode & 1) != 0:
					references = pub_field.pop('references',None)
					# Computing the stats
					pub_field['reference_stats'] = None  if references is None else self._citrefStats(references)
		elif verbosityLevel > 1:
			for pub_field in pub_list:
				if (mode & 2) != 0:
					citations = pub_field.get('citations')
					if citations is not None:
						self.populatePubIds(citations)
					
				if (mode & 1) != 0:
					references = pub_field.get('references')
					if references is not None:
						self.populatePubIds(references)
				
				if verbosityLevel >=2:
					self.listReconcileCitRefMetricsBatch(citations,verbosityLevel-1,mode)
	
