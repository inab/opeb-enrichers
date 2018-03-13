#!/usr/bin/python

import json
import sys
import os

import argparse

# Next method is used to label methods as deprecated


from libs.pub_common import *
from libs.opeb_queries import OpenEBenchQueries
from libs.europepmc_enricher import EuropePMCEnricher

#############
# Main code #
#############

if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("-F","--full", help="Return the full gathered citation results, not the citation stats by year", action="store_true")
	dof_group = parser.add_mutually_exclusive_group(required=True)
	dof_group.add_argument("-d","--directory", help="Store each separated result in the given directory", nargs=1, dest="results_dir")
	dof_group.add_argument("-f","--file", help="The results file, in JSON format",nargs=1,dest="results_file")
	parser.add_argument("cacheDir", help="The optional cache directory, to be reused", nargs="?", default=os.path.join(os.getcwd(),"cacheDir"))
	args = parser.parse_args()
	
	# Now, let's work!
	output_file = args.results_file[0] if args.results_file is not None else None
	cache_dir = args.cacheDir  
	# Creating the cache directory, in case it does not exist
	os.makedirs(os.path.abspath(cache_dir),exist_ok=True)
	with EuropePMCEnricher(cache_dir) as pub:
		# Step 1: fetch the entries with associated pubmed
		opeb_q = OpenEBenchQueries()
		fetchedEntries = opeb_q.fetchPubIds()

		# Step 2: reconcile the DOI <-> PubMed id of the entries
		results_dir = args.results_dir[0] if args.results_dir is not None else None
		if results_dir is not None:
			os.makedirs(os.path.abspath(results_dir),exist_ok=True)
		entries = pub.reconcilePubIds(fetchedEntries,results_dir,not args.full)

		#print(len(fetchedEntries))
		#print(json.dumps(fetchedEntries,indent=4))
		if output_file is not None:
			with open(output_file,mode="w",encoding="utf-8") as o:
				json.dump(entries,o,indent=4,sort_keys=True)
