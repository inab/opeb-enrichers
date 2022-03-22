#!/usr/bin/env python3

# Code borrowed from sibling project. See:
# https://github.com/inab/opeb-enrichers/blob/e137537b0e2d8d80f69f7874c47e57d9c123b94b/pubEnricher/libs/pub_common.py#L59-L75

import datetime
import http.client
import json
from typing import Any, List, Mapping, Optional, Tuple, Union
from urllib import request

# This method does the different reads and retries
# in case of partial contents
def full_http_read(req: request.Request) -> bytes:
	# The original bytes
	response = b''
	while True:
		try:
			# Try getting it
			responsePart = req.read()
		except http.client.IncompleteRead as icread:
			# Getting at least the partial content
			response += icread.partial
			continue
		else:
			# In this case, saving all
			response += responsePart
		break

	return response

def get_opener_with_auth(top_level_url: str, username: str, password: str) -> request.OpenerDirector:
	"""
	Taken from https://stackoverflow.com/a/44239906
	"""
	
	# create a password manager
	password_mgr = request.HTTPPasswordMgrWithPriorAuth()
	
	# Add the username and password.
	# If we knew the realm, we could use it instead of None.
	password_mgr.add_password(None, top_level_url, username, password, is_authenticated=True)
	
	handler = request.HTTPBasicAuthHandler(password_mgr)

	# create "opener" (OpenerDirector instance)
	return request.build_opener(handler)

class ResultJSONEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime.datetime):
			return obj.isoformat()
		elif isinstance(obj, datetime.timedelta):
			return format_duration(obj)
		
		# Let the base class default method raise the TypeError
		return super().default(obj)

# This method was borrowed from next gist
# https://gist.github.com/spatialtime/c1924a3b178b4fe721fe406e0bf1a1dc#file-iso8601_duration-py-L11-L28
def format_duration(td: datetime.timedelta) -> str:
	"""Formats a timedelta instance into a correct ISO 8601 duration string.
	Args:
		td: a datetime.timedelta instance.
	Returns:
		a ISO 8601 duration string.
	"""
	
	s = td.seconds 
	
	ms = td.microseconds
	if ms != 0:  # Round microseconds to milliseconds.
		ms /= 1000000
		ms = round(ms,3)
		s += ms
	
	return "P{}DT{}S".format(td.days, s)

# Next implementation of datetime.datetime.fromisoformat has been
# borrowed from cpython, so the code does not depend on Python 3.7+
# https://github.com/python/cpython/blob/998ae1fa3fb05a790071217cf8f6ae3a928da13f/Lib/datetime.py#L1721
def datetimeFromISOFormat(date_string: str) -> datetime.datetime:
	"""Construct a datetime from the output of datetime.isoformat()."""
	if not isinstance(date_string, str):
		raise TypeError('fromisoformat: argument must be str')
	
	# Split this at the separator
	dstr = date_string[0:10]
	tstr = date_string[11:]
	
	try:
		date_components = _parse_isoformat_date(dstr)
	except ValueError:
		raise ValueError(f'Invalid isoformat string: {date_string!r}')
	
	if tstr:
		try:
			time_components = _parse_isoformat_time(tstr)
		except ValueError:
			raise ValueError(f'Invalid isoformat string: {date_string!r}')
	else:
		time_components = [0, 0, 0, 0, None]
	
	return datetime.datetime(*date_components,*time_components)

def _parse_isoformat_date(dtstr: str) -> Tuple[int, int, int]:
	# It is assumed that this function will only be called with a
	# string of length exactly 10, and (though this is not used) ASCII-only
	year = int(dtstr[0:4])
	if dtstr[4] != '-':
		raise ValueError('Invalid date separator: %s' % dtstr[4])
	
	month = int(dtstr[5:7])
	
	if dtstr[7] != '-':
		raise ValueError('Invalid date separator')
	
	day = int(dtstr[8:10])
	
	return (year, month, day)

def _parse_isoformat_time(tstr: str) -> List[Union[int, datetime.timezone]]:
	# Format supported is HH[:MM[:SS[.fff[fff]]]][+HH:MM[:SS[.ffffff]]]
	len_str = len(tstr)
	if len_str < 2:
		raise ValueError('Isoformat time too short')
	
	# This is equivalent to re.search('[+-]', tstr), but faster
	tz_pos = (tstr.find('-') + 1 or tstr.find('+') + 1 or tstr.find('Z') + 1)
	timestr = tstr[:tz_pos-1] if tz_pos > 0 else tstr
	
	time_comps = _parse_hh_mm_ss_ff(timestr)
	
	tzi = None
	if tz_pos > 0:
		tzstr = tstr[tz_pos:]
	
		# Valid time zone strings are:
		# HH:MM               len: 5
		# HH:MM:SS            len: 8
		# HH:MM:SS.ffffff     len: 15

		if len(tzstr) == 0:
			if tstr[tz_pos - 1] != 'Z':
				raise ValueError('Malformed time zone string')
			tzi = datetime.timezone.utc
		elif (len(tzstr) not in (5, 8, 15)) or tstr[tz_pos - 1] == 'Z':
			raise ValueError('Malformed time zone string')
		else:
			tz_comps = _parse_hh_mm_ss_ff(tzstr)
			if all(x == 0 for x in tz_comps):
				tzi = datetime.timezone.utc
			else:
				tzsign = -1 if tstr[tz_pos - 1] == '-' else 1
				
				td = datetime.timedelta(
					hours=tz_comps[0],
					minutes=tz_comps[1],
					seconds=tz_comps[2],
					microseconds=tz_comps[3]
				)
				
				tzi = datetime.timezone(tzsign * td)
		
	time_comps.append(tzi)
	
	return time_comps

def _parse_hh_mm_ss_ff(tstr: str) -> List[int]:
	# Parses things of the form HH[:MM[:SS[.fff[fff]]]]
	len_str = len(tstr)
	
	time_comps = [0, 0, 0, 0]
	pos = 0
	for comp in range(0, 3):
		if (len_str - pos) < 2:
			raise ValueError('Incomplete time component')

		time_comps[comp] = int(tstr[pos:pos+2])
		
		pos += 2
		next_char = tstr[pos:pos+1]
		
		if not next_char or comp >= 2:
			break

		if next_char != ':':
			raise ValueError('Invalid time separator: %c' % next_char)
		
		pos += 1
	
	if pos < len_str:
		if tstr[pos] != '.':
			raise ValueError('Invalid microsecond component')
		else:
			pos += 1
			
			len_remainder = len_str - pos
			if len_remainder not in (3, 6):
				raise ValueError('Invalid microsecond component')
			
			time_comps[3] = int(tstr[pos:])
			if len_remainder == 3:
				time_comps[3] *= 1000

	return time_comps

def jsonFilterEncode(colKwArgs: Any, sort_keys: bool = False, indent: Optional[int] = None) -> str:
	"""
	
	"""
	return json.dumps(colKwArgs, cls=ResultJSONEncoder, sort_keys=sort_keys, indent=indent)
	
def jsonFilterStreamEncode(colKwArgs: Any, fp, sort_keys: bool = False, indent: Optional[int] = None) -> str:
	"""
	
	"""
	json.dump(colKwArgs, fp=fp, cls=ResultJSONEncoder, sort_keys=sort_keys, indent=indent)
	
def load_with_datetime(pairs: Mapping[str, Any], tz: Optional[datetime.timezone] = None) -> Mapping[str, Any]:
	"""Load with dates"""
	d = {}
	for k, v in pairs:
		if isinstance(v, str):
			try:
				dv = datetimeFromISOFormat(v)
				if tz is not None:
					dv = dv.astimezone(tz)
				d[k] = dv
			except Exception as e:
				# We are not translating the date, so continue
				d[k] = v
		else:
			d[k] = v             
	return d

def jsonFilterDecode(strColKwArgs: str,tz: Optional[datetime.timezone] =None) -> Any:
	"""
	
	"""
	return json.loads(strColKwArgs,object_pairs_hook=lambda x: load_with_datetime(x,tz))

