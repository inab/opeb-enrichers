#!/usr/bin/python

import json
import time

from urllib import request
from urllib import parse
from urllib.error import *

from typing import overload, Tuple, List, Dict, Any

from .abstract_pub_enricher import AbstractPubEnricher

from .pub_cache import PubCache

from . import pub_common

class PubmedEnricher(AbstractPubEnricher):
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
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ESummary_
	PUB_ID_SUMMARY_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi'
	def populatePubIdsBatch(self,mappings:List[Dict[str,Any]]) -> None:
		if len(mappings) > 0:
			internal_ids = [ mapping['id'] for mapping in mappings ]
			theQuery = {
				'db': 'pubmed',
				'id': ','.join(internal_ids),
				'retmode': 'json',
				'retmax': len(internal_ids),
				'rettype': 'abstract'
			}
			with request.urlopen(PUB_ID_SUMMARY_URL,data=parse.urlencode(theQuery).encode('utf-8')) as entriesConn:
				raw_pubmed_mappings = entriesConn.read()
				pubmed_mappings = json.loads(raw_pubmed_mappings.decode('utf-8'))
				
				results = pubmed_mappings.get('result')
				if results is not None:
					internal_ids_dict = { mapping['id']: mapping  for mapping in mappings }
					for result in results.values():
						_id = result.get('uid')
						if _id is not None:
							mapping = internal_ids_dict.get(_id)
							#mapping = {
							#	'id': _id,
							##	'title': result['title'],
							##	'journal': result.get('journalTitle'),
							#	'source': 'pubmed',
							#	'query': query_str,
							##	'year': int(result['pubYear']),
							##	'pmid': pubmed_id,
							##	'doi': doi_id,
							##	'pmcid': pmc_id
							#}
							mapping['source'] = 'pubmed'
							mapping['title'] = result.get('title')
							mapping['journal'] = result.get('fulljournalname')
							
							# Computing the publication year
							pubdate = result.get('pubdate')
							epubdate = result.get('epubdate')
							pubyear = None
							
							if pubdate is not None:
								pubyear = int(pubdate.split(' ')[0])
								
							if epubdate is not None:
								epubyear = int(epubdate.split(' ')[0])
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
			
			with request.urlopen(PUB_ID_CONVERTER_URL,data=parse.urlencode(theIdQuery).encode('utf-8')) as entriesConn:
				raw_id_mappings = entriesConn.read()
				id_mappings = json.loads(raw_id_mappings.decode('utf-8'))

				# We record the unpaired DOIs
				eresult = doi_mappings.get('esearchresult')
				if eresult is not None:
					idlist = eresult.get('idlist')
					translationstack = eresult.get('translationstack')
					
					# First, record the 
					if idlist is not None and translationstack is not None:
						for _id , query_str in zip(idlist,translationstack):
							# This is a very minimal mapping
							# needed to enrich and relate
							mapping = {
								'id': _id,
								#'query': query_str,
							}
							mappings.append(mapping)
				
				# print(json.dumps(entries,indent=4))
				# Avoiding to hit the server too fast
				time.sleep(0.25)
				
				# Step two: get all the information of these input queries
				self.populatePubIds(mapping_pairs)
		
		return mappings
	
	# Documented at: https://www.ncbi.nlm.nih.gov/books/NBK25499/#_chapter4_ELink_
	# The drawback of this service is that it merges the answers from several queries
	ELINKS_URL='https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi'
	def queryCitationsBatch(self,query_citations_data:List[Dict[str,Any]]) -> List[Dict[str,Any]]:
		new_citations=[]
		
		raw_ids = list(map(lambda query: query['id'], query_citations_data))
		theLinksQuery = {
			'dbfrom': 'pubmed',
			'linkname': 'pubmed_pubmed_citedin',
			'id': raw_ids,
			'db': 'pubmed',
			'tool': 'opeb-enrichers',
			'retmode': 'json'
		}
		
		with request.urlopen(ELINKS_URL,data=parse.urlencode(theLinksQuery,doseq=True).encode('utf-8')) as elinksConn:
			raw_json_citation_refs = elinksConn.read()
			raw_json_citations = json.loads(raw_json_citation_refs.decode('utf-8'))

			linksets = raw_json_citations.get('linksets')
			if linksets is not None:
				query_hash = { query['id']: query  for query in query_citations_data }
				for linkset in linksets:
					ids = linkset.get('ids',[])
					if len(ids) > 0:
						_id = ids[0]
						linksetdbs = linkset.get('linksetdbs',[])
						if len(linksetdbs) > 0:
							source_id = query['source']
							linksetdb = linksetdbs[0]
							links = linksetdb.get('links',[])
							query = query_hash[_id]
							
							citations = list(map(lambda _id: {
								'id': _id,
								'source': source_id
							},links))
							
							self.populatePubIds(citations,onlyYear=True)
							
							cite_res = {
								'id': _id,
								'source': query['source'],
								'citations': citations,
								'citation_count': len(citations)
							}
					
					
		for pub_field in query_citations_data:
			_id = pub_field.get('id') #11932250
			if _id is not None:
				source_id = pub_field['source']
				
				theLinksQuery = {
					'dbfrom': 'pubmed',
					'linkname': 'pubmed_pubmed_citedin',
					'id': _id,
					'db': 'pubmed',
					'tool': 'opeb-enrichers',
					'retmode': 'json'
				}
				
				with request.urlopen(ELINKS_URL,data=parse.urlencode(theLinksQuery,doseq=True).encode('utf-8')) as elinksConn:
					raw_json_citation_refs = elinksConn.read()
					raw_json_citations = json.loads(raw_json_citation_refs.decode('utf-8'))
					
							
		
		return new_citations
