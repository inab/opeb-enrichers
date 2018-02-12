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

# Next method is used to label methods as deprecated

import warnings
import functools

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

class PubEnricher:
	OPENEBENCH_SOURCE="https://openebench.bsc.es/monitor/rest/search?projection=publications"
	DEFAULT_CACHE_FILE="pubEnricher.shelve"
	DEFAULT_CACHE_PUB_IDS_FILE="pubEnricherIds.shelve"

	def __init__(self,cache_dir="."):
		self.cache_dir = cache_dir
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		self.cache_file = os.path.join(cache_dir,PubEnricher.DEFAULT_CACHE_FILE)
		self.cache_ids_file = os.path.join(cache_dir,PubEnricher.DEFAULT_CACHE_PUB_IDS_FILE)

	def __enter__(self):
		self.cache = shelve.open(self.cache_file)
		self.cache_ids = shelve.open(self.cache_ids_file)
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.cache.close()
		self.cache_ids.close()

	def parseOpenEBench(self,entries):
		"""
			This method takes as input a list of entries fetched from OpenEBench,
			and it returns a a list of dictionaries, whose keys are
			- id (entry id)
			- pubmed_ids
			- doi_ids
		"""
		trimmedEntries = []
		for entry in entries:
			pubmed_ids = []
			doi_ids = []
			pubs = []
			for pub in entry['publications']:
				if pub is not None:
					if 'pmid' in pub and pub['pmid'] is not None:
						pubmed_ids.append(pub['pmid'])
					if 'doi' in pub and pub['doi'] is not None:
						doi_ids.append(pub['doi'])
			if len(pubmed_ids) > 0 or len(doi_ids) > 0:
				trimmedEntries.append({
					'id': entry['@id'],
					'pubmed_ids': pubmed_ids,
					'doi_ids': doi_ids,
					'pubs': pubs
				})

		return trimmedEntries

	def fetchPubIds(self,sourceURL=OPENEBENCH_SOURCE):
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

	# Documentation at: https://europepmc.org/RestfulWebService#search
	# Documentation at: https://europepmc.org/docs/EBI_Europe_PMC_Web_Service_Reference.pdf
	DEFAULT_STEP_SIZE = 50
	OPENPMC_SEARCH_URL = 'https://www.ebi.ac.uk/europepmc/webservices/rest/search'
	def reconcilePubIdsBatch(self,entries,batch_size):
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'pubs' key, it returns the same
			parameter as input
		"""
		flatten = lambda l: [item for sublist in l for item in sublist]

		# First, gather all the ids on one list, prepared for the query
		# MED: prefix has been removed because there are some problems
		# on the server side
		
		p2e = {}
		d2e = {}
		pubmed_pairs = []
		
		pub_ids = []
		for sublist in map(lambda entry: entry['pubmed_ids'],entries):
			for pubmed_id in sublist:
				if pubmed_id in self.cache_ids:
					mapping = self.cache_ids[pubmed_id]
					pubmed_pairs.append(mapping)
					p2e[pubmed_id] = mapping
					if mapping['doi'] is not None:
						d2e[mapping['doi'].upper()] = mapping
				else:
					pub_ids.append(pubmed_id)
		
		doi_ids = []
		for sublist in map(lambda entry: entry['doi_ids'],entries):
			for doi_id in sublist:
				if doi_id not in d2e:
					if doi_id.upper() in self.cache_ids:
						mapping = self.cache_ids[doi_id.upper()]
						pubmed_pairs.append(mapping)
						d2e[doi_id.upper()] = mapping
						if mapping['pmid'] is not None:
							p2e[mapping['pmid']] = mapping
					else:
						doi_ids.append('DOI:"'+doi_id+'"')
		
		pub_ids.extend(doi_ids)

		if len(pub_ids) > 0:
			try:
				theQuery = {
					'format': 'json',
					'pageSize': 1000,
					'query': ' or '.join(pub_ids)
				}
				with request.urlopen(self.OPENPMC_SEARCH_URL+'?'+parse.urlencode(theQuery,encoding='utf-8')) as entriesConn:
					raw_json_pubs_mappings = entriesConn.read()
					#debug_cache_filename = os.path.join(self.debug_cache_dir,str(self._debug_count) + '.json')
					#self._debug_count += 1
					#with open(debug_cache_filename,mode="wb") as d:
					#	d.write(raw_json_pubs_mappings)
					
					pubs_mappings = json.loads(raw_json_pubs_mappings.decode('utf-8'))

					if 'resultList' in pubs_mappings:
						resultList = pubs_mappings['resultList']
						if 'result' in resultList:
							# Gathering results
							for result in resultList['result']:
								pubmed_id = None
								doi_id = None
								if 'pmid' in result:
									pubmed_id = result['pmid']
								if 'doi' in result:
									doi_id = result['doi']
								mapping = {
									'id': result['id'],
									'pmid': pubmed_id,
									'doi': doi_id
								}
								
								# Cache management
								if pubmed_id is not None and pubmed_id not in self.cache_ids:
									self.cache_ids[pubmed_id] = mapping
								
								if doi_id is not None and doi_id.upper() not in self.cache_ids:
									self.cache_ids[doi_id.upper()] = mapping
									
								if pubmed_id is not None:
									p2e[pubmed_id] = mapping
								if doi_id is not None:
									d2e[doi_id.upper()] = mapping
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
			pubmed_set = set(entry['pubmed_ids'])
			doi_set = set(map(lambda doi: doi.upper(), entry['doi_ids']))

			for pubmed_id  in entry['pubmed_ids']:
				if pubmed_id in p2e:
					result = p2e[pubmed_id]
					pubmed_set.discard(pubmed_id)
					if result['doi'] is not None:
						doi_set.discard(result['doi'].upper())
					entry['pubs'].append(result)

			copy_doi_set = doi_set.copy()
			for doi_id in copy_doi_set:
				if doi_id.upper() in d2e:
					result = d2e[doi_id.upper()]
					doi_set.discard(result['doi'].upper())
					if result['pmid'] is not None:
						pubmed_set.discard(result['pmid'])
					
					entry['pubs'].append(result)
			# Now, pubmed_set and doi_set contains the missing ids
			for pubmed_id in pubmed_set:
				entry['pubs'].append({
					'id': None,
					'pmid': pubmed_id,
					'doi': None
				})

			for doi_id in doi_set:
				entry['pubs'].append({
					'id': None,
					'pmid': None,
					'doi': doi_id
				})

	def parseCiteList(self,cite_res):
		"""
			iterates over the citation list and keeps only fields from the the fields
			list specified below
		"""
		fields = ['id','source','pubYear','journalAbbreviation']
		if 'citationList' in cite_res:
			if 'citation' in cite_res['citationList']:
				cite_list = cite_res['citationList']['citation']
				for cite in cite_list:
					for key in list(cite):
						if key in fields:
							pass
						else:
							del cite[key]
		return cite_list






	# Documentation at: https://europepmc.org/RestfulWebService#cites
	#Url used to retrive the citations, i.e MED is publications from PubMed and MEDLINE view https://europepmc.org/RestfulWebService;jsessionid=7AD7C81CF5F041840F59CF49ABB29994#cites
	CITATION_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/MED/"

	def reconcileCitationMetricsBatch(self,entries):
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
			if entry['pubs'] is not None:
				for pub_field in entry['pubs']:
					if pub_field['id'] is not None:
						_id = pub_field['id'] #11932250
						citations = None
						citations_cache_id = 'c'+str(_id)
						citations_count = None
						citations_count_cache_id = 'cc'+str(_id)
						if citations_cache_id in self.cache and citations_count_cache_id in self.cache:
							citations = self.cache[citations_cache_id]
							citations_count = self.cache[citations_count_cache_id]
						else:
							page = 1
							pageSize = 1000
							_format = "json"
							query = "citations"

							try:
								with request.urlopen(self.CITATION_URL+str(_id)+"/citations/"+str(page)+"/"+str(pageSize)+"/"+str(_format)) as entriesConn:
									cite_res = json.loads(entriesConn.read().decode('utf-8'))
									if 'hitCount' in cite_res:
										citations_count = cite_res['hitCount']
										if citations_count == 0 :
											citations = []
										else:
											citations = self.parseCiteList(cite_res)
											if citations_count > pageSize:
												pages = math.ceil(citations_count/pageSize)+page
												for i in range(2,pages):
													with request.urlopen(self.CITATION_URL+str(_id)+"/citations/"+str(i)+"/"+str(pageSize)+"/"+str(_format)) as entriesConn:
														cite_res = json.loads(entriesConn.read().decode('utf-8'))
														citations.extend(self.parseCiteList(cite_res))
										
										self.cache[citations_count_cache_id] = citations_count
										self.cache[citations_cache_id] = citations
									# Avoiding to be banned
									time.sleep(0.25)
							except Exception as anyEx:
								print("ERROR: Something went worng",file=sys.stderr)
								print(anyEx,file=sys.stderr)
						
						# Computing the stats
						citation_stats = {}
						for citation in citations:
							year = citation['pubYear']
							if year in citation_stats:
								citation_stats[year] += 1
							else:
								citation_stats[year] = 1
						pub_field['citation_count'] = citations_count
						pub_field['citation_stats'] = citation_stats

		# print(json.dumps(entry, indent=4))





	def reconcilePubIds(self,entries,step_size=DEFAULT_STEP_SIZE):
		"""
			This method reconciles, for each entry, the pubmed ids
			and the DOIs it has. As it manipulates the entries, adding
			the reconciliation to 'pubs' key, it returns the same
			parameter as input
		"""
		entry_batch = []

		for start in range(0,len(entries),step_size):
			self.reconcilePubIdsBatch(entries[start:(start+step_size)],step_size)
			self.reconcileCitationMetricsBatch(entries[start:(start+step_size)])
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
	if len(sys.argv) > 2:
		output_file = sys.argv[1]
		cache_dir = sys.argv[2]
		# Creating the cache directory, in case it does not exist
		os.makedirs(os.path.abspath(cache_dir),exist_ok=True)
		with PubEnricher(cache_dir) as pub:
			# Step 1: fetch the entries with associated pubmed
			fetchedEntries = pub.fetchPubIds()

			# Step 2: reconcile the DOI <-> PubMed id of the entries
			entries = pub.reconcilePubIds(fetchedEntries)

			#print(len(fetchedEntries))
			#print(json.dumps(fetchedEntries,indent=4))
			with open(output_file,mode="w",encoding="utf-8") as o:
				json.dump(entries,o,indent=4)
	else:
		print("Usage: {0} results_file cacheDir\n".format(sys.argv[0]),file=sys.stderr)
