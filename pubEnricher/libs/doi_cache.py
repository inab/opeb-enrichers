#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import datetime
from typing import Tuple, Dict, Any, NewType, Iterator

from . import pub_common
from .pub_common import Timestamps

# Alias types declaration
DOIId = NewType('DOIId',str)
DOIHandle = NewType('DOIHandle',Dict[str,Any])

import json
import sqlite3
import zlib

import re


class DOIChecker(object):
	"""
		The DOI caching checker
		Currently, it stores the check of a DOI against 
		PMC ids, DOIs and the internal identifier in the
		original source.
		Also, it stores the title, etc..
		Also, it stores the citations fetched from the original source
	"""
	POS_CACHE_DAYS = 180
	NEG_CACHE_DAYS = 7
	
	# See https://www.doi.org/factsheets/DOIHandle.html
	# also https://www.doi.org/factsheets/DOIProxy.html
	# also https://www.doi.org/doi_handbook/3_Resolution.html
	# also https://www.doi.org/doi_handbook/5_Applications.html
	
	DOI_HANDLE_ENDPOINT='https://doi.org/api/handles/'
	
	DEFAULT_CHECK_DB_FILE="DOIcheck_CACHE.db"
	
	def __init__(self,cache_dir:str="."):
		self.cache_dir = cache_dir
		
		#self.debug_cache_dir = os.path.join(cache_dir,'debug')
		#os.makedirs(os.path.abspath(self.debug_cache_dir),exist_ok=True)
		#self._debug_count = 0
		
		self.check_db_file = os.path.join(cache_dir,self.DEFAULT_CHECK_DB_FILE)
		self.jd = json.JSONDecoder()
		self.je = json.JSONEncoder()
	
	def __enter__(self):
		existsCache = os.path.exists(self.check_db_file) and (os.path.getsize(self.check_db_file) > 0)
		initializeCache = not existsCache
		
		# Opening / creating the database, with normal locking
		# and date parsing
		self.conn = sqlite3.connect(self.check_db_file, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES, check_same_thread = False)
		self.conn.execute("""PRAGMA locking_mode = NORMAL""")
		self.conn.execute("""PRAGMA journal_mode = WAL""")
		
		# Database structures
		with self.conn:
			cur = self.conn.cursor()
			updateDatabase = initializeCache
			if initializeCache:
				# Tables for DOI checks
				cur.execute("""
CREATE TABLE doi_check (
	doi VARCHAR(4096) NOT NULL,
	payload BLOB NOT NULL,
	valid_until TIMESTAMP NOT NULL
)
""")
				cur.execute("""
CREATE INDEX IF NOT EXISTS doi_check_doi ON doi_check(doi COLLATE NOCASE)
""")
			
			cur.close()
			
			
		return self
	
	def __exit__(self, exc_type, exc_val, exc_tb) -> None:
		self.conn.close()
	
	
	DOI_PATTERN = re.compile('^doi:\s*(.*)',re.I)
	
	@classmethod
	def normalize_doi(cls,doi_id):
		"""
		If the method returns None, it means the input
		is not a valid DOI
		"""
		found_pat = cls.DOI_PATTERN.search(doi_id)
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
	
	@classmethod
	def doi2curie(doi_id):
		return str(doi_id) if doi_id.startswith('doi:') else 'doi:'+doi_id
	
	def check_normalize_doi(self,doi_id):
		"""
		If the method returns None, it means the input
		is not a valid DOI
		"""
		
		# First, normalize it
		doi_id_norm = self.normalize_doi(doi_id)
		
		doi_id_alt = doi_id_norm[:-1]  if doi_id_norm[-1] == '.'  else  doi_id_norm+'.'
		
		
		
	def getRawCachedResolutions_TL(self,doi_list:Iterator[DOIId]) -> Iterator[Tuple[datetime.datetime,DOIHandle]]:
		"""
			This method does not invalidate the cache
		"""
		
		cur = self.conn.cursor()
		for doi in doi_list:
			doi_alt = doi[:-1]  if doi[-1] == '.'  else  doi+'.'
			
			cur.execute("""
SELECT valid_until, payload
FROM doi_check
WHERE
doi = :id
OR
doi = :id_alt
""",{'id': doi,'id_alt': doi_alt})
			res = cur.fetchone()
			if res:
				yield Timestamps.UTCTimestamp(res[0]), self.jd.decode(zlib.decompress(res[1]).decode("utf-8"))
			else:
				yield None, None
	
	def getRawCachedResolutions(self,doi_list:Iterator[DOIId]) -> Iterator[Tuple[datetime.datetime,DOIHandle]]:
		"""
			This method does not invalidate the cache
		"""
		
		with self.conn:
			for res_timestamp, resolution in self.getRawCachedResolutions_TL(doi_list):
				yield res_timestamp, resolution
	
	def getRawCachedResolution_TL(self,doi:DOIId) -> Tuple[datetime.datetime,DOIHandle]:
		for res_timestamp, resolution in self.getRawCachedResolutions_TL([doi]):
			return res_timestamp, resolution
	
	def getRawCachedResolution(self,doi:DOIId) -> Tuple[datetime.datetime,DOIHandle]:
		for res_timestamp, resolution in self.getRawCachedResolutions([doi]):
			return res_timestamp, resolution
	
	def getCachedResolution(self,doi:DOIId) -> DOIHandle:
		res_timestamp , resolution = self.getRawCachedResolution(doi)
		
		# Invalidate cache
		if res_timestamp is not None and (Timestamps.UTCTimestamp() > res_timestamp):
			resolution = None
		
		return resolution
	
	def setCachedResolutions(self,res_iter:Iterator[DOIHandle],valid_timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		for resolution in res_iter:
			# Before anything, get the previous mapping before updating it
			doi = resolution['handle']
			
			with self.conn:
				cur = self.conn.cursor()
				params = {
					'doi': resolution['doi'],
					'payload': zlib.compress(self.je.encode(resolution).encode("utf-8"),zlib.Z_BEST_COMPRESSION),
					'valid_until': valid_timestamp
				}
				
				# First, remove all the data from the previous resolutions
				cur.execute("""
DELETE FROM doi_check
WHERE doi = :doi
""",params)
				
				# Then, insert
				cur.execute("""
INSERT INTO doi_check(doi,payload,valid_until) VALUES(:doi,:payload,:valid_until)
""",params)
	
	def setCachedResolution(self,resolution:DOIHandle,res_timestamp:datetime.datetime = Timestamps.UTCTimestamp()) -> None:
		self.setCachedResolution([resolution],res_timestamp)
	