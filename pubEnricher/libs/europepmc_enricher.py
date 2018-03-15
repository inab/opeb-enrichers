#!/usr/bin/python

import sys
import json
import time
import math
import re

from urllib import request
from urllib import parse
from urllib.error import *

from typing import overload, Tuple, List, Dict, Any

from .abstract_pub_enricher import AbstractPubEnricher

from .pub_cache import PubCache

from . import pub_common

class EuropePMCEnricher(AbstractPubEnricher):
	@overload
	def __init__(self,cache:str="."):
		...
	
	@overload
	def __init__(self,cache:PubCache):
		...
	
	def __init__(self,cache):
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		super().__init__(cache)
	
	# Documentation at: https://europepmc.org/RestfulWebService#search
	# Documentation at: https://europepmc.org/docs/EBI_Europe_PMC_Web_Service_Reference.pdf
	OPENPMC_SEARCH_URL = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
	
	def populatePubIdsBatch(self,partial_mappings:List[Dict[str,Any]]) -> None:
		if len(partial_mappings) > 0:
			# Preparing the query ids
			raw_query_ids = []
			
			for partial_mapping in partial_mappings:
				_id = partial_mapping.get('id')
				source_id = partial_mapping.get('source')
				raw_query_ids.append('( EXT_ID:"'+_id+'" SRC:"'+source_id+'" )')
			
			# Now, with the unknown ones, let's ask the server
			theQuery = {
				'format': 'json',
				'pageSize': 1000,
				'query': ' or '.join(raw_query_ids)
			}
			#print(self.OPENPMC_SEARCH_URL+'?'+parse.urlencode(theQuery,encoding='utf-8'),file=sys.stderr)
			#sys.exit(1)
			with request.urlopen(self.OPENPMC_SEARCH_URL+'?'+parse.urlencode(theQuery,encoding='utf-8')) as entriesConn:
				raw_json_pubs_mappings = entriesConn.read()
				
				#debug_cache_filename = os.path.join(self.debug_cache_dir,str(self._debug_count) + '.json')
				#self._debug_count += 1
				#with open(debug_cache_filename,mode="wb") as d:
				#	d.write(raw_json_pubs_mappings)
				
				pubs_mappings = json.loads(raw_json_pubs_mappings.decode('utf-8'))
				
				resultList = pubs_mappings.get('resultList')
				if resultList is not None and 'result' in resultList:
					internal_ids_dict = { (partial_mapping['id'],partial_mapping['source']): partial_mapping  for partial_mapping in partial_mappings }
					# Gathering results
					for result in resultList['result']:
						_id = result['id']
						source_id = result['source']
						partial_mapping = internal_ids_dict.get((_id,source_id))
						if partial_mapping is not None:
							pubmed_id = result.get('pmid')
							doi_id = result.get('doi')
							pmc_id = result.get('pmcid')
							authors = list(filter(lambda author: len(author) > 0 , re.split(r"[.,]+\s*",result.get('authorString',""))))
							
							partial_mapping['title'] = result['title']
							partial_mapping['journal'] = result.get('journalTitle')
							partial_mapping['year'] = int(result['pubYear'])
							partial_mapping['authors'] = authors
							partial_mapping['pmid'] = pubmed_id
							partial_mapping['doi'] = doi_id
							partial_mapping['pmcid'] = pmc_id
				
				time.sleep(0.25)

				# print(json.dumps(entries,indent=4))
	
	def queryPubIdsBatch(self,query_ids:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		# Preparing the query ids
		raw_query_ids = []
		
		for query_id in query_ids:
			pubmed_id = query_id.get('pmid')
			if pubmed_id is not None:
				raw_query_ids.append('(EXT_ID:"'+pubmed_id+'" SRC:MED)')
			
			doi_id_norm = query_id.get('doi')
			if doi_id_norm is not None:
				raw_query_ids.append('DOI:"'+doi_id_norm+'"')
			
			pmc_id = query_id.get('pmcid')
			if pmc_id is not None:
				raw_query_ids.append('PMCID:"'+pmc_id+'"')
		
		# Now, with the unknown ones, let's ask the server
		mappings = []
		if len(raw_query_ids) > 0:
			theQuery = {
				'format': 'json',
				'pageSize': 1000,
				'query': ' or '.join(raw_query_ids)
			}
			print(self.OPENPMC_SEARCH_URL+'?'+parse.urlencode(theQuery,encoding='utf-8'),file=sys.stderr)
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
						authors = list(filter(lambda author: len(author) > 0 , re.split(r"[.,]+\s*",result.get('authorString',""))))
						
						if pubmed_id is not None or doi_id is not None or pmc_id is not None:
							mapping = {
								'id': _id,
								'title': result['title'],
								'journal': result.get('journalTitle'),
								'source': source_id,
								'year': int(result['pubYear']),
								'authors': authors,
								'pmid': pubmed_id,
								'doi': doi_id,
								'pmcid': pmc_id
							}
							
							mappings.append(mapping)
				
				time.sleep(0.25)

				# print(json.dumps(entries,indent=4))
		
		#json.dump(mappings,sys.stderr,indent=4,sort_keys=True)
		#sys.exit(1)
		
		return mappings
	
	def parseCiteList(self,cite_res):
		"""
			iterates over the citation list and keeps only fields from the the fields
			list specified below
		"""
		fields = ['id','source']
		filtered_cites = [ ]
		if 'citationList' in cite_res:
			if 'citation' in cite_res['citationList']:
				cite_list = cite_res['citationList']['citation']
				for cite in cite_list:
					filtered_cite = { field: cite[field] for field in filter(lambda field: field in cite , fields) }
					pubYear = cite['pubYear']
					if pubYear is not None:
						filtered_cite['year'] = int(pubYear)
					
					filtered_cites.append(filtered_cite)
		return filtered_cites
	
	# Documentation at: https://europepmc.org/RestfulWebService#cites
	#Url used to retrive the citations, i.e MED is publications from PubMed and MEDLINE view https://europepmc.org/RestfulWebService;jsessionid=7AD7C81CF5F041840F59CF49ABB29994#cites
	CITATION_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/"
	def queryCitationsBatch(self,query_citations_data:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		new_citations=[]
		for pub_field in query_citations_data:
			_id = pub_field.get('id') #11932250
			if _id is not None:
				source_id = pub_field['source']
				
				pageSize = 1000
				_format = "json"
				query = "citations"

				page = 1
				citations_count = None
				pages = None
				citations = []
				while page > 0:
					partialURL = '/'.join(map(lambda elem: parse.quote(str(elem),safe='') , [source_id,_id,"citations",page,pageSize,_format]))
					print(parse.urljoin(self.CITATION_URL,partialURL),file=sys.stderr)
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
						
				new_citations.append({
					'id': _id,
					'source': source_id,
					'citations': citations,
					'citation_count': citations_count
				})
		
		return new_citations
