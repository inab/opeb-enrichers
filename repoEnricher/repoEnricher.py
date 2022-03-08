#!/usr/bin/env python3

import argparse
from collections.abc import Iterable
import configparser
import json
import logging
import os
import sys
import traceback

from repo_enricher.common import jsonFilterStreamEncode, jsonFilterEncode
from repo_enricher.openebench_queries import OpenEBenchQueries
from repo_enricher.repo_enricher import RepoEnricher
from repo_enricher.repo_matcher.abstract import AbstractRepoMatcher

# KeyOrder

TabKeyOrder = [
	('tool_id','Tool id'),
	('vcs_type','Repository Type'),
	('vcs_uri','sourcecode repository'),
	('source_uri','sourcecode'),
	('source_isFree','sourcecode freeness'),
	('source_isDownloadRegistered','sourcecode download registration'),
	('source_hasAnonymousAccess','sourcecode anonymous access'),
	('source_isRepoBrowsable','sourcecode repository browse'),
	('source_hasVcsRepo','Source_code repository version controlled'),
	('source_isRepoAccessible','Source code repository accessible'),
	
	('has_website','website'),
	('website','website url'),
	('desc','canonical:description'),
	('concept','canonical:concept'),
	('has_help','canonical|version:HELP'),
	('has_tutorial','canonical|version:Tutorial'),
	('has_issue_tracker','issue tracker'),
	
	('binary_uri','binary distribution'),
	('binary_isDistributable','binary distribution freeness'),
	('binary_isDownloadRegistered','binaries download registration'),
	('tool_versions','Versions'),
	
	('tool_buildSystem','type'),
	
	('tool_license_uri','project license'),
	('source_license_uri','sourcecode license'),
	('source_license','sourcecode license name'),
	('is_opensource','open source'),
	('is_OSI','OSI'),
	
	# This must be the last
	('tool_developers','credits')
];

LOGGING_FORMAT = '%(asctime)-15s - [%(levelname)s] %(message)s'
DEBUG_LOGGING_FORMAT = '%(asctime)-15s - [%(name)s %(funcName)s %(lineno)d][%(levelname)s] %(message)s'

#############
# Main code #
#############

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	
	parser.add_argument('--log-file', dest="logFilename", help='Store messages in a file instead of using standard error and standard output')
	parser.add_argument('-q', '--quiet', dest='logLevel', action='store_const', const=logging.WARNING, help='Only show warnings and errors')
	parser.add_argument('-v', '--verbose', dest='logLevel', action='store_const', const=logging.INFO, help='Show verbose (informational) messages')
	parser.add_argument('-d', '--debug', dest='logLevel', action='store_const', const=logging.DEBUG, help='Show debug messages (use with care, as it could show non disclosable data)')
	
	parser.add_argument('-s', '--save-opeb', dest='save_opeb_filename', help="Save the OpenEBench content to a file")
	input_group = parser.add_mutually_exclusive_group(required=False)
	input_group.add_argument('-u', '--use-opeb', dest='load_opeb_filename', help="Use the OpenEBench content from a file instead of network")
	input_group.add_argument('--repos', dest='load_repos_filename', help="Use the list of repos from this file to query them")
	
	parser.add_argument('-C', '--config', dest='config_filename', help="Config file to pass setup parameters to the different enrichers")
	parser.add_argument('-f', '--file', dest='tabfile', help="Destination file (tabular file)")
	parser.add_argument('-D', '--directory', dest='jsondir', help="Destination directory (json files)")
	
	args = parser.parse_args()
	
	# Setting up the log
	logLevel = logging.INFO
	if args.logLevel:
		logLevel = args.logLevel
	
	if logLevel < logging.INFO:
		logFormat = DEBUG_LOGGING_FORMAT
	else:
		logFormat = LOGGING_FORMAT
	
	loggingConf = {
		'format': logFormat,
		'level': logLevel
	}
	
	if args.logFilename is not None:
		loggingConf['filename'] = args.logFilename
	#	loggingConf['encoding'] = 'utf-8'
	
	logging.basicConfig(**loggingConf)

	config = configparser.ConfigParser()
	if args.config_filename:
		try:
			config.read(args.config_filename)
		except:
			traceback.print_exc()
			sys.exit(1)
	
	jsondir = args.jsondir
	tabfile = args.tabfile
	if (jsondir is not None) or (tabfile is not None):
		
		rEnricher= RepoEnricher(config)
		reposH = None
		if args.load_repos_filename is not None:
			reposH = open(args.load_repos_filename, mode="r", encoding="utf-8")
			p_queries = rEnricher.analyzeRepositoriesList(map(lambda repourl: repourl.strip(), reposH))
		else:
			opEb = OpenEBenchQueries(args.load_opeb_filename, args.save_opeb_filename)
			p_queries = rEnricher.analyzeOpenEBenchEntries(opEb)
		
		if isinstance(p_queries, Iterable):
			jsonManifestFile = None
			manifest = list()
			TAB = None
			
			if jsondir is not None:
				os.makedirs(args.jsondir, exist_ok=True)
				jsonManifestFile = os.path.join(jsondir, 'manifest.json')
				print(f"* JSON output directory set to {jsondir} . Manifest file is {jsonManifestFile}")
			
			if tabfile is not None:
				print(f"* Tabular output file set to {tabfile}")
				try:
					TAB = open(tabfile, mode="w", encoding="utf-8")
				except Exception as e:
					raise Exception(f"ERROR: Unable to create {tabfile} (see backtrace)") from e
			elif jsondir is None:
				print("* Default to tabular output file set to STDOUT")
				TAB = sys.stdout
			
			print(f"* Processing tools")
			
			printedHeader = False
			numTool = 0
			for entry_id, entry_links, query_l in p_queries:
				for numRes, query in enumerate(query_l):
					# What we know, just now
					fullans = query.copy()
					
					tool_id = fullans.get('@id', '-')
					
					for fullrepo in fullans['repos']:
						# Removing this
						rm = fullrepo.pop('instance')
						p_ans = fullrepo['res'] = rm.getRepoData(fullrepo)
						
						if TAB is not None:
							if not printedHeader:
								printedHeader = True
								print("\t".join(map(lambda x: x[1], TabKeyOrder)), file=TAB);
								print("\t".join(map(lambda x: x[0], TabKeyOrder)), file=TAB);
							
							# Synthetic
							p_ans.setdefault('tool_id', tool_id)
							
							def _stringify(x):
								key = x[0];
								retval = p_ans.get(key, '')
								if (retval is not None):
									if isinstance(retval, (list, tuple)):
										if len(retval) > 0:
											if isinstance(retval[0], (dict,list,tuple)):
												retval = ' , '.join(map(lambda under: jsonFilterEncode(under, sort_keys=True), retval))
											else:
												retval = ' , '.join(retval)
									
									elif isinstance(retval, dict):
										retval = json.dumps(retval)
									# TODO: better checks
									# use Data::Dumper;
									# print STDERR "BUGREF\n",Dumper($retval),"\n";
									# exit(1);
									else:
										retval = str(retval)
								
								return retval
							
							print("\t".join(map(_stringify, TabKeyOrder)), file=TAB)
					
					# The assembled answer
					if jsondir is not None:
						partialJsonout = 'tool-' + str(numTool) + '_' + str(numRes) + '.json'
						manifest.append({
							'@id': fullans['@id'],
							'file': partialJsonout
						})
						jsonout = os.path.join(jsondir, partialJsonout)
						try:
							with open(jsonout, mode="w", encoding="utf-8") as J:
								jsonFilterStreamEncode(fullans, fp=J, sort_keys=True, indent=4)
						except Exception as e:
							import pprint
							pprint.pprint(fullans)
							raise Exception(f"* ERROR: Unable to create file {jsonout}") from e
			
				# Another more
				numTool += 1
			
			print(f"* Processed {numTool} tools")
			
			# Closing the output file (if any)
			if TAB not in (sys.stdout, None):
				TAB.close()
			
			# Writing the manifest
			if jsonManifestFile is not None:
				try:
					with open(jsonManifestFile, mode='w', encoding='utf-8') as M:
						json.dump(manifest, fp=M, sort_keys=True, indent=4)
				except Exception as e:
					raise Exception(f"ERROR: Unable to write manifest {jsonManifestFile}") from e
		else:
			print("No queries extracted from input (either OpenEBench or list of repos). Do you have internet access?", file=sys.stderr);
		
	else:
		parser.print_help()
		sys.exit(1)
