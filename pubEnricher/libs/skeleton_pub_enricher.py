#!/usr/bin/python

import sys
import os
import json
import configparser
import copy

from urllib import request
from urllib.error import *
import socket

import datetime
import time

from abc import ABC, abstractmethod

from typing import overload, Tuple, List, Dict, Any, Iterator

from .pub_cache import PubCache

from . import pub_common

class SkeletonPubEnricher(ABC):
	DEFAULT_STEP_SIZE = 50
	DEFAULT_NUM_FILES_PER_DIR = 1000
	
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
		self.num_files_per_dir = self.config.getint(section_name,'num_files_per_dir',fallback=self.DEFAULT_NUM_FILES_PER_DIR)
		
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
	def listReconcileCitRefMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0,mode:int=3) -> None:
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
	
	def listReconcileRefMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of found publications and retrieves citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		self.listReconcileCitRefMetricsBatch(pub_list,verbosityLevel,1)
	
	def listReconcileCitMetricsBatch(self,pub_list:List[Dict[str,Any]],verbosityLevel:float=0) -> None:
		"""
			This method takes in batches of found publications and retrieves citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""
		self.listReconcileCitRefMetricsBatch(pub_list,verbosityLevel,2)

	# This method does the different reads and retries
	# in case of partial contents
	def retriable_full_http_read(self,theRequest:request.Request,timeout:int=300,debug_url=None) -> bytes:
		if self._debug and (debug_url is not None):
			print(debug_url,file=sys.stderr)
			sys.stderr.flush()
		retries = 0
		while retries <= self.max_retries:
			try:
				# The original bytes
				response = b''
				with request.urlopen(theRequest,timeout=timeout) as req:
					while True:
						try:
							# Try getting it
							responsePart = req.read()
						except http.client.IncompleteRead as icread:
							# Getting at least the partial content
							response += icread.partial
							continue
						else:
							# In this case, saving all
							response += responsePart
						break
				
				return response
			except HTTPError as e:
				if e.code >= 500 and retries < self.max_retries:
					# Using a backoff time of 2 seconds when 500 or 502 errors are hit
					retries += 1
					
					if self._debug:
						print("Retry {0} , due code {1}".format(retries,e.code),file=sys.stderr)
					
					time.sleep(2**retries)
				else:
					if debug_url is not None:
						print("URL with ERROR: "+debug_url+"\n",file=sys.stderr)
						sys.stderr.flush()
					raise e
			except socket.timeout as e:
				# Using also a backoff time of 2 seconds when read timeouts occur
				retries += 1
				
				if self._debug:
					print("Retry {0} , due timeout".format(retries),file=sys.stderr)
				
				time.sleep(2**retries)
	
	def _citrefStats(self,citrefs:Iterator[Dict[str,Any]]) -> List[Dict[str,Any]]:
		# Computing the stats
		citref_stats = {}
		for citref in citrefs:
			year = citref.get('year',-1)
			if year is None:
				year = -1
			if year in citref_stats:
				citref_stats[year] += 1
			else:
				citref_stats[year] = 1
		
		return [ {'year':year,'count':citref_stats[year]} for year in sorted(citref_stats.keys()) ]
	
	def flattenPubs(self,opeb_entries:List[Dict[str,Any]]) -> None:
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
		
		return linear_pubs
	
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
	
	def _getUniqueNewPubs(self,query_pubs:List[Dict[str,Any]],query_refs:List[Dict[str,Any]],saved_pubs:Dict[str,str],saved_comb:Dict[str,str]):
		# The list of new citations to populate later
		if len(query_pubs) > 0:
			new_pubs = list(filter(lambda pub: (pub.get('source') is not None) and ((pub.get('source','') + ':' + pub.get('id','')) not in saved_pubs),query_pubs))
		else:
			new_pubs = []
		
		unique_pubs = {}
		unique_comb_pubs = {}
		for new_pub in new_pubs:
			new_key = new_pub.get('source','') + ':' + new_pub.get('id','')
			if new_key not in unique_pubs:
				unique_pubs[new_key] = new_pub
				unique_comb_pubs[new_key] = new_pub
		
			
		#import pprint
		#pp = pprint.PrettyPrinter(indent=4)
		#pp.pprint(query_pubs)
		
		if len(query_refs) > 0:
			#pp.pprint(query_refs)
			new_ref_pubs = list(filter(lambda pub: (pub.get('source') is not None) and ((pub.get('source','') + ':' + pub.get('id','')) not in saved_comb),query_refs))
		else:
			new_ref_pubs = []
		
		for new_ref_pub in new_ref_pubs:
			new_comb_key = new_ref_pub.get('source','') + ':' + new_ref_pub.get('id','')
			if new_comb_key not in unique_comb_pubs:
				unique_comb_pubs[new_comb_key] = new_ref_pub
		
		if len(unique_comb_pubs) == 0:
			return None, None
		
		# The list to obtain the basic publication data
		# and the list of new citations to dig in later (as soft as possible)
		
		return list(unique_comb_pubs.values()),list(unique_pubs.values())
	
	def reconcilePubIds(self,entries:List[Dict[str,Any]],results_path:str=None,results_format:str=None,verbosityLevel:float=0) -> List[Any]:
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'found_pubs' key, it returns the same
			parameter as input
		"""
		
		# As flat format is so different from the previous ones, use a separate codepath
		if results_format == "flat":
			# This unlinks the input from the output
			copied_entries = copy.deepcopy(entries)
			
			# The tools subdirectory
			tools_subpath = 'tools'
			os.makedirs(os.path.abspath(os.path.join(results_path,tools_subpath)),exist_ok=True)
			
			saved_tools = []
			# Now, gather the tool publication entries
			filename_prefix = 'pub_tool_'
			for start in range(0,len(copied_entries),self.step_size):
				stop = start+self.step_size
				entries_slice = copied_entries[start:stop]
				self.reconcilePubIdsBatch(entries_slice)
				
				copied_entries_slice = copy.deepcopy(entries_slice)
				for idx, entry in enumerate(copied_entries_slice):
					part_dest_file = os.path.join(tools_subpath,filename_prefix+str(start+idx)+'.json')
					dest_file = os.path.join(results_path,part_dest_file)
					saved_tools.append({
						'@id': entry['@id'],
						'file': part_dest_file
					})
					with open(dest_file,mode="w",encoding="utf-8") as outentry:
						json.dump(entry,outentry,indent=4,sort_keys=True)
			
			# Recording what we have already fetched (and saved)
			saved_pubs = {}
			saved_comb = {}
			saved_comb_arr = []
			
			# The counter for the files being generated
			pub_counter = 0
			pubs_subpath = 'pubs'
			query_refs = []
			query_pubs = self.flattenPubs(copied_entries)
			
			depth = 0
			
			while (len(query_pubs) + len(query_refs)) > 0 and depth < verbosityLevel:
				unique_to_populate , unique_to_reconcile = self._getUniqueNewPubs(query_pubs,query_refs,saved_pubs,saved_comb)
				
				query_pubs = []
				query_refs = []
				if unique_to_populate is None:
					break
				
				print("DEBUG: Level {} Pop {} Rec {}".format(depth,len(unique_to_populate),len(unique_to_reconcile)),file=sys.stderr)
				sys.stderr.flush()
				
				# Obtaining the publication data
				self.populatePubIds(unique_to_populate)
				self.listReconcileRefMetricsBatch(unique_to_populate,-1)
				
				# The list of new citations to dig in later (as soft as possible)
				self.listReconcileCitMetricsBatch(unique_to_reconcile,-1)
				
				# Saving (it works because all the elements in unique_to_reconcile are in unique_to_populate)
				# and getting the next batch from those with references and/or citations
				for new_pub in unique_to_populate:
					# Getting the name of the file
					new_key = new_pub.get('source','') + ':' + new_pub.get('id','')
					
					assert new_key not in saved_pubs
					if new_key in saved_comb:
						new_pub_file = saved_comb[new_key]
					else:
						if pub_counter % self.num_files_per_dir == 0:
							pubs_subpath = 'pubs_'+str(pub_counter)
							os.makedirs(os.path.abspath(os.path.join(results_path,pubs_subpath)),exist_ok=True)
						part_new_pub_file = os.path.join(pubs_subpath,'pub_'+str(pub_counter)+'.json')
						saved_comb_arr.append({
							'_id': new_key,
							'file': part_new_pub_file
						})
						new_pub_file = os.path.join(results_path,part_new_pub_file)
						pub_counter += 1
					
					reconciled = False
					if 'references' in new_pub:
						reconciled = True
						if new_pub['references'] is not None:
							query_refs.extend(new_pub['references'])
						# Fixing the output
						new_pub['reference_refs'] = new_pub.pop('references')
					if 'citations' in new_pub:
						reconciled = True
						if new_pub['citations'] is not None:
							query_pubs.extend(new_pub['citations'])
						# Fixing the output
						new_pub['citation_refs'] = new_pub.pop('citations')
					
					with open(new_pub_file,mode="w",encoding="utf-8") as outentry:
						json.dump(new_pub,outentry,indent=4,sort_keys=True)
					
					saved_comb[new_key] = new_pub_file
					if reconciled:
						saved_pubs[new_key] = new_pub_file
				
				depth += 1
			
			# Last but one, the border condition
			if (len(query_pubs) + len(query_refs)) > 0:
				unique_to_populate , unique_to_reconcile = self._getUniqueNewPubs(query_pubs,query_refs,saved_pubs,saved_comb)
				
				if unique_to_populate is not None:
					print("DEBUG: Last Pop {}".format(len(unique_to_populate)),file=sys.stderr)
					sys.stderr.flush()
					# Obtaining the publication data
					self.populatePubIds(unique_to_populate)
					self.listReconcileRefMetricsBatch(unique_to_populate,-1)
					
					for new_pub in unique_to_populate:
						# Getting the name of the file
						new_key = new_pub.get('source','') + ':' + new_pub.get('id','')
						
						assert new_key not in saved_pubs
						if new_key in saved_comb:
							new_pub_file = saved_comb[new_key]
						else:
							if pub_counter % self.num_files_per_dir == 0:
								pubs_subpath = 'pubs_'+str(pub_counter)
								os.makedirs(os.path.abspath(os.path.join(results_path,pubs_subpath)),exist_ok=True)
							part_new_pub_file = os.path.join(pubs_subpath,'pub_'+str(pub_counter)+'.json')
							saved_comb_arr.append({
								'_id': new_key,
								'file': part_new_pub_file
							})
							new_pub_file = os.path.join(results_path,part_new_pub_file)
							pub_counter += 1
							
						if 'references' in new_pub:
							# Fixing the output
							new_pub['reference_refs'] = new_pub['references']
							del new_pub['references']
						
						with open(new_pub_file,mode="w",encoding="utf-8") as outentry:
							json.dump(new_pub,outentry,indent=4,sort_keys=True)
						
						saved_comb[new_key] = new_pub_file
			
			print("DEBUG: Saved {} publications".format(pub_counter),file=sys.stderr)
			sys.stderr.flush()
			
			# Last, save the manifest file
			manifest_file = os.path.join(results_path,'manifest.json')
			with open(manifest_file,mode="w",encoding="utf-8") as manifile:
				json.dump({'@timestamp': datetime.datetime.now().isoformat(), 'tools': saved_tools, 'publications': saved_comb_arr},manifile,indent=4,sort_keys=True)
		else:
			#print(len(fetchedEntries))
			#print(json.dumps(fetchedEntries,indent=4))
			if results_format == "single":
				jsonOutput = open(results_path,mode="w",encoding="utf-8")
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
				elif results_format == "multiple":
					filename_prefix = 'entry_' if verbosityLevel == 0  else 'fullentry_'
					for idx, entry in enumerate(entries_slice):
						dest_file = os.path.join(results_path,filename_prefix+str(start+idx)+'.json')
						with open(dest_file,mode="w",encoding="utf-8") as outentry:
							json.dump(entry,outentry,indent=4,sort_keys=True)
			
			if jsonOutput is not None:
				print(']',file=jsonOutput)
				jsonOutput.close()
		
		return entries
