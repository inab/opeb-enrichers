#!/usr/bin/python

import os
import json

from abc import ABC, abstractmethod

from typing import overload, Tuple, List, Dict, Any

from .pub_cache import PubCache

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
	
	@abstractmethod
	def reconcilePubIdsBatch(self,entries:List[Any]) -> None:
		pass
	
	@abstractmethod
	def reconcileCitationMetricsBatch(self,entries:List[Any],digestStats:bool=True) -> None:
		pass
	
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
			if results_dir is not None:
				for idx, entry in enumerate(entries_slice):
					dest_file = os.path.join(results_dir,'entry_'+str(start+idx)+'.json')
					with open(dest_file,mode="w",encoding="utf-8") as outentry:
						json.dump(entry,outentry,indent=4,sort_keys=True)
		
		return entries
