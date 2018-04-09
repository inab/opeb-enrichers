#!/usr/bin/python

import sys
import os
import json
import configparser

from abc import ABC, abstractmethod

from typing import overload, Tuple, List, Dict, Any, Iterator

from .pub_cache import PubCache

from . import pub_common

class SkeletonPubEnricher(ABC):
	DEFAULT_STEP_SIZE = 50
	
	@overload
	def __init__(self,cache:str=".",prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	@overload
	def __init__(self,cache:PubCache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	def __init__(self,cache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		if type(cache) is str:
			self.cache_dir = cache
			self.pubC = PubCache(self.cache_dir,prefix=prefix)
		else:
			self.pubC = cache
			self.cache_dir = cache.cache_dir
		
		# Load at least a config parser
		self.config = config if config else configparser.ConfigParser()
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		# Adding empty sections, in order to avoid the NoSectionError exception
		if not self.config.has_section(section_name):
			self.config.add_section(section_name)
		
		self.step_size = self.config.getint(section_name,'step_size',fallback=self.DEFAULT_STEP_SIZE)
		
		# Debug flag
		self._debug = debug
		
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		super().__init__()
	
	def __enter__(self):
		self.pubC.__enter__()
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.pubC.__exit__(exc_type, exc_val, exc_tb)
	
	@classmethod
	@abstractmethod
	def Name(cls) -> str:
		return 'skel'
	
	@abstractmethod
	def reconcilePubIdsBatch(self,entries:List[Any]) -> None:
		pass
	
	@abstractmethod
	def reconcileCitRefMetricsBatch(self,entries:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of entries and retrives citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		pass
	
	def reconcilePubIds(self,entries:List[Any],results_dir:str=None,verbosityLevel:float=0) -> List[Any]:
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'found_pubs' key, it returns the same
			parameter as input
		"""
		
		for start in range(0,len(entries),self.step_size):
			stop = start+self.step_size
			entries_slice = entries[start:stop]
			self.reconcilePubIdsBatch(entries_slice)
			self.reconcileCitRefMetricsBatch(entries_slice,verbosityLevel)
			self.pubC.sync()
			if results_dir is not None:
				filename_prefix = 'entry_' if verbosityLevel == 0  else 'fullentry_'
				for idx, entry in enumerate(entries_slice):
					dest_file = os.path.join(results_dir,filename_prefix+str(start+idx)+'.json')
					with open(dest_file,mode="w",encoding="utf-8") as outentry:
						json.dump(entry,outentry,indent=4,sort_keys=True)
		
		return entries
