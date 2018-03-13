#!/usr/bin/python

import json
from urllib import request
from urllib import parse
from urllib.error import *
import sys
import os
import time
import math
import shelve

import argparse

# Next method is used to label methods as deprecated

import warnings
import functools

import datetime, time
import re

from typing import overload, Tuple, List, Dict, Any

def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emitted
    when the function is used."""
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func

#############
# Main code #
#############

def doi2curie(doi_id):
	return str(doi_id) if doi_id.startswith('doi:') else 'doi:'+doi_id

def pmid2curie(pubmed_id):
	return 'pmid:'+str(pubmed_id)

def pmcid2curie(pmc_id):
	return 'pmc:'+str(pmc_id)

DOI_PATTERN = re.compile('^doi:\s*(.*)',re.I)

def normalize_doi(doi_id):
	found_pat = DOI_PATTERN.search(doi_id)
	if found_pat:
		# It is already a CURI
		doi_id = found_pat.group(1)
	elif doi_id.startswith('http'):
		# It is an URL
		parsed_doi_id = parse.urlparse(doi_id)
		if parsed_doi_id.netloc.endswith('doi.org'):
			# Removing the initial slash
			doi_id = parsed_doi_id.path[1:]
		
	return doi_id.upper()

class OpenEBenchQueries:
	OPENEBENCH_SOURCE ="https://openebench.bsc.es/monitor/rest/search?projection=publications"
	OPEB_PUB_FIELDS = ( 'pmid' , 'doi' , 'pmcid' )
	
	def __init__(self):
		pass
	
	def parseOpenEBench(self,entries:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		"""
			This method takes as input a list of entries fetched from OpenEBench,
			and it returns a a list of dictionaries, whose keys are
			- id (entry id)
			- entry_pubs
		"""
		trimmedEntries = []
		for entry in entries:
			entry_pubs = []
			for pub in entry['publications']:
				if pub is not None:
					filtered_pub = { field: pub[field] for field in filter(lambda field: field in pub, self.OPEB_PUB_FIELDS)}
					filtered_pub['found_pubs'] = []
					if len(filtered_pub) > 0:
						entry_pubs.append(filtered_pub)
			
			if len(entry_pubs) > 0:
				trimmedEntries.append({
					'@id': entry['@id'],
					'entry_pubs': entry_pubs
				})
		
		return trimmedEntries
	
	def fetchPubIds(self,sourceURL:str=OPENEBENCH_SOURCE) -> List[Dict[str,Any]]:
		"""
			This method fetches from OpenEBench the list of publications for each
			entry, and it returns a list of dictionaries, whose keys are
			- id (entry id)
			- pubmed_idsmay get suffix added by low-le
			- doi_ids
			The reconciliation is done later
		"""
		try:
			with request.urlopen(sourceURL) as resp:
				retval = json.loads(resp.read().decode('utf-8'))

				return self.parseOpenEBench(retval)

		except URLError as ue:
			print("ERROR: could not fetch {0}".format(sourceURL),file=sys.stderr)
		except json.JSONDecodeError as jde:
			print("ERROR: Bad-formed JSON: "+jde.msg)
		except Exception as anyEx:
			print("Something unexpected happened",file=sys.stderr)
			print(anyEx,file=sys.stderr)

class Timestamps:
	@staticmethod
	def LocalTimestamp(theDate:datetime=datetime.datetime.now()) -> datetime:
		utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
		return theDate.replace(tzinfo=datetime.timezone(offset=datetime.timedelta(seconds=-utc_offset_sec)))
	
	@staticmethod
	def UTCTimestamp(theUTCDate:datetime=datetime.datetime.utcnow()) -> datetime:
		return theUTCDate.replace(tzinfo=datetime.timezone.utc)

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
		
		self.cache_citations_file = os.path.join(cache_dir,PubCache.DEFAULT_CACHE_CITATIONS_FILE)
		self.cache_ids_file = os.path.join(cache_dir,PubCache.DEFAULT_CACHE_PUB_IDS_FILE)
		self.cache_idmaps_file = os.path.join(cache_dir,PubCache.DEFAULT_CACHE_PUB_IDMAPS_FILE)
	
	def __enter__(self):
		self.cache_citations = shelve.open(self.cache_citations_file)
		self.cache_ids = shelve.open(self.cache_ids_file)
		self.cache_idmaps = shelve.open(self.cache_idmaps_file)
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb) -> None:
		self.cache_citations.close()
		self.cache_ids.close()
		self.cache_idmaps.close()
	
	
	def getCitationsAndCount(self,source_id:str,_id:str) -> Tuple[List[Dict[str,Any]],int]:
		refId = source_id+':'+_id
		citations_timestamp , citations , citations_count = self.cache_citations.get(refId,(None,None,None))
		
		# Invalidate cache
		if citations_timestamp is not None and (Timestamps.UTCTimestamp() - citations_timestamp) > self.OLDEST_CACHE:
			citations = None
			citation_count = None
		
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
	
	def setCachedMapping(self,source_id:str,_id:str,mapping:Dict[str,Any],mapping_timestamp:datetime = Timestamps.UTCTimestamp()) -> None:
		refId = source_id+':'+_id
		old_mapping_timestamp , old_mapping = self.getRawCachedMapping(source_id,_id)
		
		# First, cleanup of sourceIds cache
		if old_mapping_timestamp is not None:
			pubmed_id = old_mapping.get('pmid')
			if pubmed_id is not None:
				self.removeSourceId(pubmed_id,source_id,_id,mapping_timestamp)
			
			doi_id = old_mapping.get('doi')
			if doi_id is not None:
				doi_id_norm = normalize_doi(doi_id)
				self.removeSourceId(doi_id_norm,source_id,_id,mapping_timestamp)
			
			pmc_id = old_mapping.get('pmcid')
			if pmc_id is not None:
				self.removeSourceId(pmc_id,source_id,_id,mapping_timestamp)
		
		# Then, store
		self.cache_idmaps[refId] = (mapping_timestamp,mapping)
	
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
		

class PubEnricher:
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
	
	def __enter__(self):
		self.pubC.__enter__()
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb):
		self.pubC.__exit__(exc_type, exc_val, exc_tb)
	
	# Documentation at: https://europepmc.org/RestfulWebService#search
	# Documentation at: https://europepmc.org/docs/EBI_Europe_PMC_Web_Service_Reference.pdf
	DEFAULT_STEP_SIZE = 50
	OPENPMC_SEARCH_URL = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
	def reconcilePubIdsBatch(self,entries:List[Any],batch_size:int):
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'found_pubs' key, it returns the same
			parameter as input
		"""
		flatten = lambda l: [item for sublist in l for item in sublist]

		# First, gather all the ids on one list, prepared for the query
		# MED: prefix has been removed because there are some problems
		# on the server side
		
		p2e = {}
		pmc2e = {}
		d2e = {}
		pubmed_pairs = []
		
		def _updateCaches(publish_id):
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
						doi_id_norm = normalize_doi(doi_id)
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
				# This loop avoid resolving twice
				pubmed_id = entry_pub.get('pmid')
				if pubmed_id is not None and pubmed_id not in p2e:
					if not _updateCaches(pubmed_id):
						query_ids.append('EXT_ID:'+pubmed_id)
				
				doi_id = entry_pub.get('doi')
				if doi_id is not None:
					doi_id_norm = normalize_doi(doi_id)
					if doi_id_norm not in d2e and not _updateCaches(doi_id_norm):
						query_ids.append('DOI:"'+doi_id_norm+'"')
				
				pmc_id = entry_pub.get('pmcid')
				if pmc_id is not None and pmc_id not in pmc2e:
					if not _updateCaches(pmc_id):
						query_ids.append('PMCID:'+pmc_id)
		
		# Now, with the unknown ones, let's ask the server
		if len(query_ids) > 0:
			try:
				theQuery = {
					'format': 'json',
					'pageSize': 1000,
					'query': ' or '.join(query_ids)
				}
				with request.urlopen(self.OPENPMC_SEARCH_URL+'?'+parse.urlencode(theQuery,encoding='utf-8')) as entriesConn:
					raw_json_pubs_mappings = entriesConn.read()
					
					#debug_cache_filename = os.path.join(self.debug_cache_dir,str(self._debug_count) + '.json')
					#self._debug_count += 1
					#with open(debug_cache_filename,mode="wb") as d:
					#	d.write(raw_json_pubs_mappings)
					
					pubs_mappings = json.loads(raw_json_pubs_mappings.decode('utf-8'))
					
					resultList = pubs_mappings.get('resultList')
					if resultList is not None and 'result' in resultList:
						# Gathering results
						for result in resultList['result']:
							_id = result['id']
							pubmed_id = result.get('pmid')
							doi_id = result.get('doi')
							pmc_id = result.get('pmcid')
							source_id = result.get('source')
							
							if pubmed_id is not None or doi_id is not None or pmc_id is not None:
								europepmc_id = source_id + ':' + _id
								mapping = {
									'id': _id,
									'title': result['title'],
									'journal': result.get('journalTitle'),
									'source': source_id,
									'year': int(result['pubYear']),
									'pmid': pubmed_id,
									'doi': doi_id,
									'pmcid': pmc_id
								}
								
								# Cache management
								self.pubC.setCachedMapping(source_id,_id,mapping)
								if pubmed_id is not None:
									p2e.setdefault(pubmed_id,{})[source_id] = mapping
									self.pubC.appendSourceId(pubmed_id,source_id,_id)
								
								if pmc_id is not None:
									pmc2e.setdefault(pmc_id,{})[source_id] = mapping
									self.pubC.appendSourceId(pmc_id,source_id,_id)
								
								if doi_id is not None:
									doi_id_norm = normalize_doi(doi_id)
									d2e.setdefault(doi_id_norm,{})[source_id] = mapping
									self.pubC.appendSourceId(doi_id_norm,source_id,_id)
								
								pubmed_pairs.append(mapping)
					
					time.sleep(0.25)

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
					curie_id = pmid2curie(pubmed_id)
					initial_curie_ids.append(curie_id)
					if pubmed_id in p2e:
						results.append(p2e[pubmed_id])
					else:
						curie_id = pmid2curie(pubmed_id)
						broken_curie_ids.append(curie_id)
				
				doi_id = entry_pub.get('doi')
				if doi_id is not None:
					curie_id = doi2curie(doi_id)
					initial_curie_ids.append(curie_id)
					doi_id_norm = normalize_doi(doi_id)
					if doi_id_norm in d2e:
						results.append(d2e[doi_id_norm])
					else:
						curie_id = doi2curie(doi_id)
						broken_curie_ids.append(curie_id)
				
				pmc_id = entry_pub.get('pmcid')
				if pmc_id is not None:
					curie_id = pmcid2curie(pmc_id)
					initial_curie_ids.append(curie_id)
					if pmc_id in pmc2e:
						results.append(pmc2e[pmc_id])
					else:
						curie_id = pmcid2curie(pmc_id)
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
							curie_id = pmid2curie(pubmed_id)
							curie_ids.append(curie_id)
						
						doi_id = new_winner.get('doi')
						if doi_id is not None:
							curie_id = doi2curie(doi_id)
							curie_ids.append(curie_id)
						
						pmc_id = new_winner.get('pmcid')
						if pmc_id is not None:
							curie_id = pmcid2curie(pmc_id)
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
	
	def parseCiteList(self,cite_res):
		"""
			iterates over the citation list and keeps only fields from the the fields
			list specified below
		"""
		fields = ['id','source','pubYear','journalAbbreviation']
		filtered_cites = [ ]
		if 'citationList' in cite_res:
			if 'citation' in cite_res['citationList']:
				cite_list = cite_res['citationList']['citation']
				for cite in cite_list:
					filtered_cite = { field: cite[field] for field in filter(lambda field: field in cite , fields) }
					
					filtered_cites.append(filtered_cite)
		return filtered_cites






	# Documentation at: https://europepmc.org/RestfulWebService#cites
	#Url used to retrive the citations, i.e MED is publications from PubMed and MEDLINE view https://europepmc.org/RestfulWebService;jsessionid=7AD7C81CF5F041840F59CF49ABB29994#cites
	CITATION_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/"

	def reconcileCitationMetricsBatch(self,entries,digestStats=True):
		"""
			This method takes in batches of entries and retrives citations from ids
			hitCount: number of times cited
				for each citation it retives
					id: id of the paper it was cited in
					source: from where it was retrived i.e MED = publications from PubMed and MEDLINE
					pubYear: year of publication
					journalAbbreviation: Journal Abbriviations
		"""

		for entry in entries:
			for entry_pub in entry['entry_pubs']:
				if entry_pub['found_pubs'] is not None:
					for pub_field in entry_pub['found_pubs']:
						if pub_field['id'] is not None:
							source_id = pub_field['source']
							_id = pub_field['id'] #11932250
							
							citations, citations_count = self.pubC.getCitationsAndCount(source_id,_id)
							
							if citations is None or citations_count is None:
								pageSize = 1000
								_format = "json"
								query = "citations"

								try:
									page = 1
									citations_count = None
									pages = None
									citations = []
									while page > 0:
										partialURL = '/'.join(map(lambda elem: parse.quote(str(elem),safe='') , [source_id,_id,"citations",page,pageSize,_format]))
										with request.urlopen(parse.urljoin(self.CITATION_URL,partialURL)) as entriesConn:
											raw_json_citations = entriesConn.read()
											
											#debug_cache_filename = os.path.join(self.debug_cache_dir,'cite_' + str(self._debug_count) + '.json')
											#self._debug_count += 1
											#with open(debug_cache_filename,mode="wb") as d:
											#	d.write(raw_json_citations)
											
											cite_res = json.loads(raw_json_citations.decode('utf-8'))
											if citations_count is None:
												citations_count = 0
												if 'hitCount' in cite_res:
													citations_count = cite_res['hitCount']
												
												if citations_count == 0 :
													page = 0
													pages = 0
												else:
													pages = math.ceil(citations_count/pageSize)
												
											citations.extend(self.parseCiteList(cite_res))
											
											if page < pages:
												page += 1
												# Avoiding to be banned
												time.sleep(0.25)
											else:
												page = 0
											
									self.pubC.setCitationsAndCount(source_id,_id,citations,citations_count)
									
									#debug_cache_filename = os.path.join(self.debug_cache_dir,'parsedcite_' + str(self._debug_count) + '.json')
									#self._debug_count += 1
									#with open(debug_cache_filename,mode="w",encoding="utf-8") as d:
									#	json.dump(citations,d,indent=4)
									
								except Exception as anyEx:
									print("ERROR: Something went wrong",file=sys.stderr)
									print(anyEx,file=sys.stderr)
							
							pub_field['citation_count'] = citations_count
							if digestStats:
								# Computing the stats
								citation_stats = {}
								for citation in citations:
									year = citation['pubYear']
									if year in citation_stats:
										citation_stats[year] += 1
									else:
										citation_stats[year] = 1
								pub_field['citation_stats'] = citation_stats
							else:
								pub_field['citations'] = citations

		# print(json.dumps(entry, indent=4))





	def reconcilePubIds(self,entries,results_dir=None,digestStats=True,step_size=DEFAULT_STEP_SIZE):
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'found_pubs' key, it returns the same
			parameter as input
		"""
		entry_batch = []

		for start in range(0,len(entries),step_size):
			stop = start+step_size
			entries_slice = entries[start:stop]
			self.reconcilePubIdsBatch(entries_slice,step_size)
			self.reconcileCitationMetricsBatch(entries_slice,digestStats)
			if results_dir is not None:
				for idx, entry in enumerate(entries_slice):
					dest_file = os.path.join(results_dir,'entry_'+str(start+idx)+'.json')
					with open(dest_file,mode="w",encoding="utf-8") as outentry:
						json.dump(entry,outentry,indent=4,sort_keys=True)
		return entries


####################################
## Next methods are obsolete code ##
####################################

PUB_ID_SUMMARY_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
@deprecated
def reconcilePubmedIdsBatch(pubmed_ids,batch_size=PubEnricher.DEFAULT_STEP_SIZE):
	theQuery = {
		'db': 'pubmed',
		'id': ','.join(pubmed_ids),
		'retmode': 'json',
		'retmax': batch_size,
		'rettype': 'abstract'
	}
	p2d = {}
	d2p = {}
	pubmed_pairs = []
	with request.urlopen(PUB_ID_SUMMARY_URL,data=parse.urlencode(theQuery).encode('utf-8')) as entriesConn:
		pubmed_mappings = json.loads(entriesConn.read().decode('utf-8'))

		if 'result' in pubmed_mappings:
			results = pubmed_mappings['result']
			for result in results.values():
				if 'articleids' in result:
					pubmed_id = None
					doi_id = None
					for articleid in result['articleids']:
						if 'idtype' in articleid  and 'value' in articleid:
							if articleid['idtype'] == 'pubmed':
								pubmed_id = articleid['value']
							elif articleid['idtype'] == 'doi':
								doi_id = articleid['value']
					mapping = {
						'pmid': pubmed_id,
						'doi': doi_id
					}

					pubmed_pairs.append(mapping)
					if pubmed_id is not None:
						p2d[pubmed_id] = mapping
					if doi_id is not None:
						d2p[doi_id] = mapping

		#print(json.dumps(pubmed_mappings,indent=4))
		# sys.exit(1)
	return p2d , d2p , pubmed_pairs

PUB_ID_CONVERTER_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
@deprecated
def reconcileDOIIdsBatch(doi_ids,batch_size=PubEnricher.DEFAULT_STEP_SIZE):
	theQuery = {
		'db': 'pubmed',
		'term': ' OR '.join(doi_ids),
		'format': 'json',
		'field': 'doi'
	}
	doi_p2d = {}
	doi_d2p = {}
	pubmed_pairs = []
	with request.urlopen(PUB_ID_CONVERTER_URL,data=parse.urlencode(theQuery).encode('utf-8')) as entriesConn:
		doi_mappings = json.loads(entriesConn.read().decode('utf-8'))

		# We record the unpaired DOIs
		if 'esearchresult' in doi_mappings:
			eresult = doi_mappings['esearchresult']
			# Registering doi ids with no mapping
			if 'errorlist' in eresult:
				errorlist = eresult['errorlist']
				if 'phrasesnotfound' in errorlist:
					for doi in errorlist['phrasesnotfound']:
						pubmed_pairs.append({
							'doi': doi,
							'pmid': None
						})
			if 'idlist' in eresult:
				doi_p2d, doi_d2p, doi_pubmed_pairs = reconcilePubmedIdsBatch(eresult['idlist'],batch_size)
				pubmed_pairs.extend(doi_pubmed_pairs)

	return doi_p2d , doi_d2p , pubmed_pairs

#######################################
## End of methods which are obsolete ##
#######################################

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-F","--full", help="Return the full gathered citation results, not the citation stats by year", action="store_true")
	dof_group = parser.add_mutually_exclusive_group(required=True)
	dof_group.add_argument("-d","--directory", help="Store each separated result in the given directory", nargs=1, dest="results_dir")
	dof_group.add_argument("-f","--file", help="The results file, in JSON format",nargs=1,dest="results_file")
	parser.add_argument("cacheDir", help="The optional cache directory, to be reused", nargs="?", default=os.path.join(os.getcwd(),"cacheDir"))
	args = parser.parse_args()
	
	# Now, let's work!
	output_file = args.results_file[0] if args.results_file is not None else None
	cache_dir = args.cacheDir  
	# Creating the cache directory, in case it does not exist
	os.makedirs(os.path.abspath(cache_dir),exist_ok=True)
	with PubEnricher(cache_dir) as pub:
		# Step 1: fetch the entries with associated pubmed
		opeb_q = OpenEBenchQueries()
		fetchedEntries = opeb_q.fetchPubIds()

		# Step 2: reconcile the DOI <-> PubMed id of the entries
		results_dir = args.results_dir[0] if args.results_dir is not None else None
		if results_dir is not None:
			os.makedirs(os.path.abspath(results_dir),exist_ok=True)
		entries = pub.reconcilePubIds(fetchedEntries,results_dir,not args.full)

		#print(len(fetchedEntries))
		#print(json.dumps(fetchedEntries,indent=4))
		if output_file is not None:
			with open(output_file,mode="w",encoding="utf-8") as o:
				json.dump(entries,o,indent=4,sort_keys=True)
