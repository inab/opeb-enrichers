#!/usr/bin/python

import sys
import os
import json
import configparser
import copy

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
	def listReconcileCitRefMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of found publications and retrieves citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		pass
	
	def _citrefStats(self,citrefs:Iterator[Dict[str,Any]]) -> List[Dict[str,Any]]:
		# Computing the stats
		citref_stats = {}
		for citref in citrefs:
			year = citref['year']
			if year in citref_stats:
				citref_stats[year] += 1
			else:
				citref_stats[year] = 1
		
		return [ {'year':year,'count':citref_stats[year]} for year in sorted(citref_stats.keys()) ]
	
	def reconcileCitRefMetricsBatch(self,opeb_entries:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of entries and retrives citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		
		linear_pubs = []
		for entry_pubs in map(lambda opeb_entry: opeb_entry['entry_pubs'],opeb_entries):
			for entry_pub in entry_pubs:
				linear_pubs.extend(entry_pub['found_pubs'])
		
		self.listReconcileCitRefMetricsBatch(linear_pubs,verbosityLevel)
	
	def reconcilePubIds(self,entries:List[Dict[str,Any]],results_dir:str=None,results_file:str=None,verbosityLevel:float=0) -> List[Any]:
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'found_pubs' key, it returns the same
			parameter as input
		"""
		
		#print(len(fetchedEntries))
		#print(json.dumps(fetchedEntries,indent=4))
		if results_file is not None:
			jsonOutput = open(results_file,mode="w",encoding="utf-8")
			print('[',file=jsonOutput)
			printComma = False
		else:
			jsonOutput = None
		
		for start in range(0,len(entries),self.step_size):
			stop = start+self.step_size
			# This unlinks the input from the output
			entries_slice = copy.deepcopy(entries[start:stop])
			self.reconcilePubIdsBatch(entries_slice)
			self.reconcileCitRefMetricsBatch(entries_slice,verbosityLevel)
			self.pubC.sync()
			if jsonOutput is not None:
				for entry in entries_slice:
					if printComma:
						print(',',file=jsonOutput)
					else:
						printComma=True
					json.dump(entry,jsonOutput,indent=4,sort_keys=True)
			if results_dir is not None:
				filename_prefix = 'entry_' if verbosityLevel == 0  else 'fullentry_'
				for idx, entry in enumerate(entries_slice):
					dest_file = os.path.join(results_dir,filename_prefix+str(start+idx)+'.json')
					with open(dest_file,mode="w",encoding="utf-8") as outentry:
						json.dump(entry,outentry,indent=4,sort_keys=True)
		
		if jsonOutput is not None:
			print(']',file=jsonOutput)
			jsonOutput.close()
		
		return entries
