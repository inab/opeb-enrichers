#!/usr/bin/env python3

import json
import logging
import lzma
from typing import Any, Iterator, List, Mapping, Optional, Tuple
from urllib import request
import urllib.error

from .common import full_http_read

# Constant and variable declaration

class OpenEBenchQueriesException(Exception):
	pass

# Static recursive method
def _linkExtract(p_entry: Mapping, p_features: Mapping) -> List:
	entry_links = list()
	
	for mkey in p_features.keys():
		if mkey in p_entry:
			s_features = p_features.get(mkey)
			if s_features is not None:
				s_entry = p_entry.get(mkey)
				if isinstance(s_entry, dict):
					entry_links.extend(_linkExtract(s_entry, s_features));
			elif isinstance(p_entry.get(mkey), list):
				for it in p_entry.get(mkey):
					if isinstance(it, str) and len(it) > 0:
						entry_links.append(it)
			elif isinstance(p_entry.get(mkey), str) and len(p_entry[mkey]) > 0:
				entry_links.append(p_entry[mkey])
	
	return entry_links

# Method bodies
class OpenEBenchQueries:
	OPENEBENCH_SOURCE = 'https://openebench.bsc.es/monitor/rest/search'
	# The features where we are going to look into
	Features = {
		'documentation': {
			'general': None,
			'manual': None
		},
		'distributions': {
			'source_packages': None,
			'binary_packages': None,
			'sourcecode': None,
			'binaries': None
		},
		'web': {
			'homepage': None
		},
		'homepage': None,
		'repositories':	None
	}
	
	def __init__(self, load_opeb_filename:str, save_opeb_filename:str, sourceURL: str = OPENEBENCH_SOURCE, selected_features: Mapping[str, Any] = Features):
		# Getting a logger focused on specific classes
		import inspect
		
		self.logger = logging.getLogger(dict(inspect.getmembers(self))['__module__'] + '::' + self.__class__.__name__)
		
		self.load_opeb_filename = load_opeb_filename
		self.save_opeb_filename = save_opeb_filename
		self.sourceURL = sourceURL
		
		self.selected_features = selected_features
	
	def extractQueryableRepoIds(self) ->  Iterator[Tuple[str, List[str]]]:
		try:
			raw_opeb = None
			if self.load_opeb_filename is not None:
				openmethod = None
				if self.load_opeb_filename.endswith('.xz'):
					openmethod = lzma.open
				else:
					openmethod = open
				with openmethod(self.load_opeb_filename, mode="rb") as XOP:
					raw_opeb = XOP.read()
			else:
				req = request.Request(self.sourceURL)
				# This fixes an issue, as the API answers in several flavours
				req.add_header('Accept','application/json')
				with request.urlopen(req) as resp:
					raw_opeb = full_http_read(resp)
				
			# Saving the payload
			if self.save_opeb_filename:
				with open(self.save_opeb_filename,mode="wb") as savefile:
					savefile.write(raw_opeb)
			
			retval = json.loads(raw_opeb.decode('utf-8'))
		
		except urllib.error.URLError as ue:
			raise OpenEBenchQueriesException(f"ERROR: could not fetch {self.sourceURL}") from ue
		except json.JSONDecodeError as jde:
			raise OpenEBenchQueriesException("ERROR: Bad-formed OpenEBench JSON: "+jde.msg) from jde
		except Exception as anyEx:
			raise OpenEBenchQueriesException("Something unexpected happened in extractQueryableRepoIds (see stack trace)") from anyEx
		
		if not isinstance(retval, list):
			retval = [ retval ]
		
		return self.extractOpenEBenchFeatures(retval)
	
	def extractOpenEBenchFeatures(self, p_entries: List[Mapping]) -> Iterator[Tuple[str, List[str]]]:
		"""
		It yields the features we are interested in
		"""
		for entry in p_entries:
			# import json, sys
			# json.dump(entry, sort_keys=True, indent=4, fp=sys.stderr)
			entry_links = _linkExtract(entry, self.selected_features)
			yield entry['@id'], entry_links
