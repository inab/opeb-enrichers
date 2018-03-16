#!/usr/bin/python

import sys
import json
from urllib import request
from urllib import parse
from urllib.error import *

from typing import Tuple, List, Dict, Any

class OpenEBenchQueries:
	OPENEBENCH_SOURCE ="https://openebench.bsc.es/monitor/rest/search?projection=publications"
	OPEB_PUB_FIELDS = ( 'pmid' , 'doi' , 'pmcid' )
	
	def __init__(self,load_opeb_filename:str=None,save_opeb_filename:str=None):
		self.load_opeb_filename = load_opeb_filename
		self.save_opeb_filename = save_opeb_filename
	
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
			if self.load_opeb_filename:
				with open(self.load_opeb_filename,mode="rb") as resp:
					raw_opeb = resp.read()
			else:
				with request.urlopen(sourceURL) as resp:
					raw_opeb = resp.read()
			
			if self.save_opeb_filename:
				with open(self.save_opeb_filename,mode="wb") as savefile:
					savefile.write(raw_opeb)
			
			retval = json.loads(raw_opeb.decode('utf-8'))

			return self.parseOpenEBench(retval)

		except URLError as ue:
			print("ERROR: could not fetch {0}".format(sourceURL),file=sys.stderr)
		except json.JSONDecodeError as jde:
			print("ERROR: Bad-formed JSON: "+jde.msg)
		except Exception as anyEx:
			print("Something unexpected happened",file=sys.stderr)
			print(anyEx,file=sys.stderr)
