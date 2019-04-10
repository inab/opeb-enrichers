#!/usr/bin/python

import sys
import os
import json
import configparser
import copy

from abc import abstractmethod

from typing import overload, Tuple, List, Dict, Any, Iterator

from .skeleton_pub_enricher import SkeletonPubEnricher
from .pub_cache import PubDBCache

from . import pub_common

class AbstractPubEnricher(SkeletonPubEnricher):
	DEFAULT_REQUEST_DELAY = 0.25
	
	@overload
	def __init__(self,cache:str=".",prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	@overload
	def __init__(self,cache:PubDBCache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		...
	
	def __init__(self,cache,prefix:str=None,config:configparser.ConfigParser=None,debug:bool=False):
		super().__init__(cache,prefix,config,debug)
		
		# The section name is the symbolic name given to this class
		section_name = self.Name()
		
		request_delay = self.config.getfloat(section_name,'request_delay',fallback=self.DEFAULT_REQUEST_DELAY)
		self.request_delay = request_delay
	
	@classmethod
	@abstractmethod
	def Name(cls) -> str:
		return 'abstract'
