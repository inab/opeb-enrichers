#!/usr/bin/python

import json
import time
import re

from urllib import request
from urllib import parse
from urllib.error import *

from typing import overload, Tuple, List, Dict, Any

from .abstract_pub_enricher import AbstractPubEnricher

from .pub_cache import PubCache

from . import pub_common

class PubmedEnricher(AbstractPubEnricher):
	@overload
	def __init__(self,cache:str=".",step_size:int=AbstractPubEnricher.DEFAULT_STEP_SIZE):
		...
	
	@overload
	def __init__(self,cache:PubCache,step_size:int=AbstractPubEnricher.DEFAULT_STEP_SIZE):
		...
	
	def __init__(self,cache,step_size:int=AbstractPubEnricher.DEFAULT_STEP_SIZE):
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		super().__init__(cache,step_size)
	
	PUBMED_SOURCE='pubmed'
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESummary_
	PUB_ID_SUMMARY_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
	def populatePubIdsBatch(self,mappings:List[Dict[str,Any]]) -> None:
		if len(mappings) > 0:
			internal_ids = [ mapping['id']  for mapping in mappings ]
			theQuery = {
				'db': 'pubmed',
				'id': ','.join(internal_ids),
				'retmode': 'json',
				'retmax': len(internal_ids),
				'rettype': 'abstract'
			}
			with request.urlopen(self.PUB_ID_SUMMARY_URL,data=parse.urlencode(theQuery).encode('utf-8')) as entriesConn:
				raw_pubmed_mappings = entriesConn.read()
				pubmed_mappings = json.loads(raw_pubmed_mappings.decode('utf-8'))
				
				results = pubmed_mappings.get('result')
				if results is not None:
					uids = results.get('uids',[])
					internal_ids_dict = { mapping['id']: mapping  for mapping in mappings }
					for uid in uids:
						_id = str(uid)
						result = results[_id]
						mapping = internal_ids_dict.get(_id)
						#mapping = {
						#	'id': _id,
						##	'title': result['title'],
						##	'journal': result.get('journalTitle'),
						#	'source': self.PUBMED_SOURCE,
						#	'query': query_str,
						##	'year': int(result['pubYear']),
						##	'pmid': pubmed_id,
						##	'doi': doi_id,
						##	'pmcid': pmc_id
						#}
						# mapping['source'] = self.PUBMED_SOURCE
						mapping['title'] = result.get('title')
						mapping['journal'] = result.get('fulljournalname')
						
						# Computing the publication year
						pubdate = result.get('pubdate')
						epubdate = result.get('epubdate')
						pubyear = None
						
						if pubdate is not None:
							pubyear = int(re.split(r"[ -]",pubdate)[0])
							
						if epubdate is not None and len(epubdate) > 0:
							epubyear = int(re.split(r"[ -]",epubdate)[0])
							if epubyear < pubyear:
								pubyear = epubyear
						
						mapping['year'] = pubyear
						
						mapping['authors'] = [ author.get('name')  for author in result.get('authors',[]) ]
						
						# Rescuing the identifiers
						pubmed_id = None
						doi_id = None
						pmc_id = None
						articleids = result.get('articleids')
						if articleids is not None:
							for articleid in result['articleids']:
								idtype = articleid.get('idtype')
								if idtype is not None:
									if idtype == 'pubmed':
										pubmed_id = articleid.get('value')
									elif idtype == 'doi':
										doi_id = articleid.get('value')
									elif idtype == 'pmc':
										pmc_id = articleid.get('value')
						
						mapping['pmid'] = pubmed_id
						mapping['doi'] = doi_id
						mapping['pmcid'] = pmc_id

				# Avoiding to hit the server too fast
				time.sleep(0.25)
				#print(json.dumps(pubmed_mappings,indent=4))
				# sys.exit(1)
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESearch_
	# Documentation: https://www.nlm.nih.gov/bsd/mms/medlineelements.html
	PUB_ID_CONVERTER_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi'
	def queryPubIdsBatch(self,query_ids:List[Dict[str,str]]) -> List[Dict[str,Any]]:
		# Preparing the query ids
		raw_query_ids = []
		
		for query_id in query_ids:
			pubmed_id = query_id.get('pmid')
			if pubmed_id is not None:
				queryStr = pubmed_id+'[pmid]'
				raw_query_ids.append(queryStr)
			
			doi_id_norm = query_id.get('doi')
			if doi_id_norm is not None:
				queryStr = '"'+doi_id_norm+'"[doi]'
				raw_query_ids.append(queryStr)
			
			pmc_id = query_id.get('pmcid')
			if pmc_id is not None:
				queryStr = pmc_id+'[pmc]'
				raw_query_ids.append(queryStr)
		
		# Now, with the unknown ones, let's ask the server
		mappings = []
		if len(raw_query_ids) > 0:
			# Step one: get the internal identifiers corresponding to the input queries
			theIdQuery = {
				'db': 'pubmed',
				'term': ' OR '.join(raw_query_ids),
				'format': 'json'
			}
			
			with request.urlopen(self.PUB_ID_CONVERTER_URL,data=parse.urlencode(theIdQuery).encode('utf-8')) as entriesConn:
				raw_id_mappings = entriesConn.read()
				id_mappings = json.loads(raw_id_mappings.decode('utf-8'))

				# We record the unpaired DOIs
				eresult = id_mappings.get('esearchresult')
				if eresult is not None:
					idlist = eresult.get('idlist')
					translationstack = eresult.get('translationstack')
					
					# First, record the 
					if idlist is not None and translationstack is not None:
						for _id , query_str in zip(idlist,translationstack):
							# This is a very minimal mapping
							# needed to enrich and relate
							mapping = {
								'id': str(_id),
								'source': self.PUBMED_SOURCE
								#'query': query_str,
							}
							mappings.append(mapping)
				
				# print(json.dumps(entries,indent=4))
				# Avoiding to hit the server too fast
				time.sleep(0.25)
				
				# Step two: get all the information of these input queries
				self.populatePubIds(mappings)
		
		return mappings
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ELink_
	# Documented at: https://eutils.ncbi.nlm.nih.gov/entrez/query/static/entrezlinks.html
	# The drawback of this service is that it we are not careful enough,
	# it merges the answers from several queries
	ELINKS_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi'
	ELINK_QUERY_MAPPINGS = {
		'pubmed_pubmed_citedin': ('citations','citation_count'),
		'pubmed_pubmed_refs': ('references','reference_count'),
	}
	
	def queryCitRefsBatch(self,query_citations_data:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		new_citations=[]
		
		raw_ids = list(map(lambda query: query['id'], query_citations_data))
		theLinksQuery = {
			'dbfrom': 'pubmed',
			'linkname': 'pubmed_pubmed_citedin,pubmed_pubmed_refs',
			'id': raw_ids,
			'db': 'pubmed',
			'tool': 'opeb-enrichers',
			'retmode': 'json'
		}
		
		with request.urlopen(self.ELINKS_URL,data=parse.urlencode(theLinksQuery,doseq=True).encode('utf-8')) as elinksConn:
			raw_json_citation_refs = elinksConn.read()
			raw_json_citations = json.loads(raw_json_citation_refs.decode('utf-8'))

			linksets = raw_json_citations.get('linksets')
			if linksets is not None:
				query_hash = { query['id']: query  for query in query_citations_data }
				for linkset in linksets:
					ids = linkset.get('ids',[])
					if len(ids) > 0:
						_id = str(ids[0])
						linksetdbs = linkset.get('linksetdbs',[])
						if len(linksetdbs) > 0:
							query = query_hash[_id]
							source_id = query['source']
							cite_res = {
								'id': _id,
								'source': source_id
							}
							for linksetdb in linksetdbs:
								linkname = linksetdb['linkname']
								
								citrefs_key,citrefs_count_key = self.ELINK_QUERY_MAPPINGS.get(linkname,(None,None))
								if citrefs_key and citrefs_key not in query:
									#import sys
									#print(query_hash,file=sys.stderr)
									links = linksetdb.get('links',[])
									
									citrefs = list(map(lambda uid: {
										'id': str(uid),	# _id
										'source': source_id
									},links))
									
									self.populatePubIds(citrefs,onlyYear=True)
									
									cite_res[citrefs_key] = citrefs
									cite_res[citrefs_count_key] = len(citrefs)
							
							new_citations.append(cite_res)
			
			# Avoiding to hit the server too fast
			time.sleep(0.25)
		
		return new_citations
