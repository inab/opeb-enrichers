#!/usr/bin/env python3

import abc
import configparser
import json
import logging
import time
from typing import Any, List, Mapping, Optional, Tuple, Union
import urllib

from ..common import get_opener_with_auth


class RepoMatcherException(Exception):
	pass

class AbstractRepoMatcher(abc.ABC):
	# Common constants

	recognizedBuildSystemsByLang = {
		'Makefile': 'make',
		'CMake': 'cmake'
	}

	recognizedInterpretedLanguages = set((
		'python',
		'perl',
		'ruby',
		'r',
		'php',
		'golang',
		'javascript',
		'shell',
		'jsoniq',
	))

	recognizedCompiledLanguages = set((
		'c',
		'c++',
		'java',
		'fortran',
		'perl 6',
		'pascal',
		'objective-c',
		'component pascal',
		'scala',
	))
	
	def __init__(self, config: configparser.ConfigParser):
		if not isinstance(config, configparser.ConfigParser):
			raise RepoMatcherException('Expected a configparser.ConfigParser instance as parameter')
		
		# Getting a logger focused on specific classes
		import inspect
		
		self.logger = logging.getLogger(dict(inspect.getmembers(self))['__module__'] + '::' + self.__class__.__name__)
		
		self.config = config
		self.req_period = None
		self._opener = self._getOpener()
	
	def reqPeriod(self):
		if self.req_period is None:
			config = self.config
			numreq = config.getint(self.kind(), 'numreq', fallback=config.getint('default', 'numreq', fallback=3600))
			self.req_period = 3600 / numreq
		
		return self.req_period
	
	@abc.abstractclassmethod
	def kind(cls) -> str:
		pass
	
	@abc.abstractmethod
	def doesMatch(self, uriStr: str) -> Tuple[bool, Optional[str], Optional[str]]:
		pass
	
	@abc.abstractmethod
	def getRepoData(self, fullrepo: Mapping[str, Any]) -> Mapping[str, Any]:
		pass
	
	@abc.abstractmethod
	def _getCredentials(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
		return None, None, None
	
	def _getOpener(self) -> urllib.request.OpenerDirector:
		user, token, top_level_url = self._getCredentials()
		
		return urllib.request.urlopen  if user is None  else  get_opener_with_auth(top_level_url, user, token).open
	
	def fetchJSON(self, bUri: Union[str, urllib.parse.ParseResult], p_acceptHeaders: Optional[str] = None, numIter: int = 0) -> Tuple[bool, List[Mapping]]:
		"""
		Shared method to fetch data from repos
		"""
		
		if isinstance(bUri, urllib.parse.ParseResult):
			uriStr = urllib.parse.urlunparse(bUri)
		else:
			uriStr = bUri
		
		bData = None
		period = self.reqPeriod()
		
		is_success = True
		while is_success:
			bUriStr = uriStr
			uriStr = None
			
			req = urllib.request.Request(bUriStr);
			if p_acceptHeaders is not None:
				req.add_header('Accept', p_acceptHeaders)
			
			# To honor the limit of 5000 requests per hour
			t0 = time.time()
			linkH = None
			try:
				with self._opener(req) as response:
					newBData = json.load(response)
					linkH = response.getheader('Link')
					
			except json.JSONDecodeError as jde:
				raise RepoMatcherException(f'JSON parsing error on {bUriStr}: {jde.msg}') from jde
			except urllib.error.HTTPError as he:
				self.logger.exception(f'Kicked out {bUriStr}: {he.code}')
				is_success = False
				#raise RepoMatcherException(f'Kicked out {bUriStr}: {he.code}') from he
			except urllib.error.URLError as ue:
				raise RepoMatcherException(f'Kicked out {bUriStr}: {ue.reason}') from ue
			except Exception as e:
				raise RepoMatcherException(f'Kicked out {bUriStr}') from e
			
			except Exception as e:
				# Show must go on
				self.logger.exception(f'Kicked out {bUriStr}')
				is_success = False
			else:
				# Assuming it is an array
				if isinstance(bData, list):
					if isinstance(newBData, list):
						bData.extend(newBData)
					else:
						bData.append(newBData)
				else:
					bData = newBData
				
				# Are we paginating?
				if isinstance(linkH, str) and len(linkH) > 0:
					if not isinstance(bData, list):
						bData = [ bData ]
					for link in linkH.split(', '):
						splitSemi = link.split('; ')
						newLink = splitSemi[0]
						newRel = splitSemi[1]  if len(splitSemi) > 0  else  None
						
						if newRel=="rel='next'":
							newLink = newLink.translate(str.maketrans('', '', '<>'))
							uriStr = newLink
							numIter -= 1
							break
			
			# Should we sleep?
			leap = time.time() - t0
			if period > leap:
				time.sleep(period - leap)
			
			# Simulating a do ... while
			if (uriStr is None) or numIter == 0:
				break
		
		return is_success , bData
