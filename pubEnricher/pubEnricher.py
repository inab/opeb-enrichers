#!/usr/bin/python

import json
import sys
import os

import argparse
import configparser

# Next method is used to label methods as deprecated


from libs.pub_common import *
from libs.opeb_queries import OpenEBenchQueries
from libs.abstract_pub_enricher import AbstractPubEnricher
from libs.europepmc_enricher import EuropePMCEnricher
from libs.pubmed_enricher import PubmedEnricher

#############
# Main code #
#############

if __name__ == "__main__":
	backends = {
		'europepmc': EuropePMCEnricher,
		'pubmed': PubmedEnricher
	}
	parser = argparse.ArgumentParser()
	parser.add_argument("-F","--full", help="Return the full gathered citation results, not the citation stats by year", action="count", default=0)
	parser.add_argument("-d","--debug", help="Show the URL statements", action="store_true", default=False)
	parser.add_argument("-b","--backend", help="Choose the enrichment backend", choices=backends, default='europepmc')
	parser.add_argument("-C","--config", help="Config file to pass setup parameters to the different enrichers", nargs=1,dest="config_filename")
	parser.add_argument("--save-opeb", help="Save the OpenEBench content to a file", nargs=1,dest="save_opeb_filename")
	parser.add_argument("--use-opeb", help="Use the OpenEBench content from a file instead of network", nargs=1,dest="load_opeb_filename")
	dof_group = parser.add_mutually_exclusive_group(required=True)
	dof_group.add_argument("-D","--directory", help="Store each separated result in the given directory", nargs=1, dest="results_dir")
	dof_group.add_argument("-f","--file", help="The results file, in JSON format",nargs=1,dest="results_file")
	parser.add_argument("cacheDir", help="The optional cache directory, to be reused", nargs="?", default=os.path.join(os.getcwd(),"cacheDir"))
	args = parser.parse_args()
	
	# Now, let's work!
	output_file = args.results_file[0] if args.results_file is not None else None
	config_filename = args.config_filename[0] if args.config_filename is not None else None
	save_opeb_filename = args.save_opeb_filename[0] if args.save_opeb_filename is not None else None
	load_opeb_filename = args.load_opeb_filename[0] if args.load_opeb_filename is not None else None
	cache_dir = args.cacheDir
	debug = args.debug
	
	# Parsing the config file
	if config_filename is None:
		config = None
	else:
		print("* Reading config file {}".format(config_filename))
		config = configparser.ConfigParser()
		config.read(config_filename)
	
	# Creating the cache directory, in case it does not exist
	os.makedirs(os.path.abspath(cache_dir),exist_ok=True)
	ChosenEnricher = backends.get(args.backend,EuropePMCEnricher)
	with ChosenEnricher(cache_dir,config,debug=debug) as pub:
		# Step 1: fetch the entries with associated pubmed
		opeb_q = OpenEBenchQueries(load_opeb_filename,save_opeb_filename)
		fetchedEntries = opeb_q.fetchPubIds()

		# Step 2: reconcile the DOI <-> PubMed id of the entries
		results_dir = args.results_dir[0] if args.results_dir is not None else None
		if results_dir is not None:
			os.makedirs(os.path.abspath(results_dir),exist_ok=True)
		entries = pub.reconcilePubIds(fetchedEntries,results_dir,args.full)

		#print(len(fetchedEntries))
		#print(json.dumps(fetchedEntries,indent=4))
		if output_file is not None:
			with open(output_file,mode="w",encoding="utf-8") as o:
				json.dump(entries,o,indent=4,sort_keys=True)
