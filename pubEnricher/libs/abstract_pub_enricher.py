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
		
		for entry_pubs in map(lambda entry: entry['entry_pubs'],entries):
			for entry_pub in entry_pubs:
				query_id = {}
				# This loop avoid resolving twice
				pubmed_id = entry_pub.get('pmid')
				if pubmed_id is not None and pubmed_id not in p2e:
					if not _updateCaches(pubmed_id):
						query_id['pmid'] = pubmed_id
				
				doi_id = entry_pub.get('doi')
				if doi_id is not None:
					doi_id_norm = pub_common.normalize_doi(doi_id)
					if doi_id_norm not in d2e and not _updateCaches(doi_id_norm):
						query_id['doi'] = doi_id_norm
				
				pmc_id = entry_pub.get('pmcid')
				if pmc_id is not None and pmc_id not in pmc2e:
					if not _updateCaches(pmc_id):
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
						self.pubC.appendSourceId(pubmed_id,source_id,_id)
					
					pmc_id = mapping.get('pmcid')
					if pmc_id is not None:
						pmc2e.setdefault(pmc_id,{})[source_id] = mapping
						self.pubC.appendSourceId(pmc_id,source_id,_id)
					
					doi_id = mapping.get('doi')
					if doi_id is not None:
						doi_id_norm = pub_common.normalize_doi(doi_id)
						d2e.setdefault(doi_id_norm,{})[source_id] = mapping
						self.pubC.appendSourceId(doi_id_norm,source_id,_id)
					
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
	def reconcileCitationMetricsBatch(self,entries:List[Dict[str,Any]],digestStats:bool=True) -> None:
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
