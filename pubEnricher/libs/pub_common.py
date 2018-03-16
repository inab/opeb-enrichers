#!/usr/bin/python

import datetime, time
from urllib import parse

class Timestamps:
	@staticmethod
	def LocalTimestamp(theDate:datetime=datetime.datetime.now()) -> datetime:
		utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
		return theDate.replace(tzinfo=datetime.timezone(offset=datetime.timedelta(seconds=-utc_offset_sec)))
	
	@staticmethod
	def UTCTimestamp(theUTCDate:datetime=datetime.datetime.utcnow()) -> datetime:
		return theUTCDate.replace(tzinfo=datetime.timezone.utc)

def print_debug_url(module,sourceURL):
	print(' '.join(module ,str(Timestamps.LocalTimestamp()),sourceURL),file=sys.stderr)


import re

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
