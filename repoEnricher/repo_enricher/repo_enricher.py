#!/usr/bin/env python3

import configparser
import importlib
import logging
from typing import Iterator, List, Mapping, Optional, Tuple

from .openebench_queries import OpenEBenchQueries
from .repo_matcher.abstract import AbstractRepoMatcher

# Constant and variable declaration
class RepoEnricherException(Exception):
	pass

class RepoEnricher:
	# These are dynamically loaded
	# from .repo_matcher.github import GitHubRepoMatcher
	# from .repo_matcher.bitbucket import BitBucketRepoMatcher
	MATCHERS = [
		('repo_enricher.repo_matcher.github', 'GitHubRepoMatcher'),
		('repo_enricher.repo_matcher.bitbucket', 'BitBucketRepoMatcher'),
	]

	def __init__(self, config: configparser.ConfigParser, p_matchers: List[AbstractRepoMatcher] = MATCHERS):
		# Getting a logger focused on specific classes
		import inspect
		
		self.logger = logging.getLogger(dict(inspect.getmembers(self))['__module__'] + '::' + self.__class__.__name__)
		
		if not isinstance(config, configparser.ConfigParser):
			raise ValueError("Expected a configparser.ConfigParser instance")
		
		rmInstances = list();
		for rmmodule_name, rmclazz_name in p_matchers:
			#my($success,$errmsg) = Class::Load::try_load_class($rmclazz);
			#if($success) {
			#	push(@rmInstances,$rmclazz->new($config,$self->{'ua'}))
			#} else {
			#	Carp::croak("Unable to load class $rmclazz . Reason: $errmsg");
			#}
			try:
				rmmodule = importlib.import_module(rmmodule_name)
				rmclazz = getattr(rmmodule, rmclazz_name)
				if not issubclass(rmclazz, AbstractRepoMatcher):
					raise ValueError(f"{rmclazz.__class__.__name__} is not a subclass of AbstractRepoMatcher")
				
				rmInstances.append(rmclazz(config))
			except Exception as e:
				raise RepoEnricherException(f"Unable to load module {rmmodule_name} or class {rmclazz_name} ({str(e)} see stack trace)") from e
		
		self.repo_matchers = rmInstances
		
	def analyzeOpenEBenchEntries(self, oebQ: OpenEBenchQueries) -> Iterator[Tuple[str, List[str], List[Mapping]]]:
		for entry_id, entry_links in oebQ.extractQueryableRepoIds():
			
			yield entry_id, entry_links, self.parsePutativeURLs(entry_id, entry_links)
	
	def analyzeRepositoriesList(self, repo_links: Iterator[str]) -> Iterator[Tuple[str, List[str], List[Mapping]]]:
		for repo_link in repo_links:
			entry_links = [ repo_link ]
			yield repo_link, entry_links, self.parsePutativeURLs(repo_link, entry_links)
	
	def parsePutativeURLs(self, entry_id: str, entry_links: List[str]) ->  List[Mapping]:
		queries = list()
		
		if len(entry_links) > 0:
			repoEntries = dict()
			repos = list()
			
			for entry_link in entry_links:
				for rm in self.repo_matchers:
					isURI, workspace, repo = rm.doesMatch(entry_link)
					
					if isURI and isinstance(workspace, str) and len(workspace) > 0 and isinstance(repo, str) and len(repo) > 0:
						# Due GitHub behaves, it is case insensitive
						lcWorkspace = workspace.lower()
						lcRepo = repo.lower()
						kind = rm.kind()
						
						p_links = repoEntries.setdefault(kind, dict()).setdefault(lcWorkspace, dict()).get(lcRepo)
						if p_links is None:
							p_links = list();
							repoEntries[kind][lcWorkspace][lcRepo] = p_links
							repos.append({
								'kind': kind,
								'instance': rm,
								'owner': workspace,
								'workspace': workspace,
								'repo': repo,
								'links': p_links
							})
						
						# Gathering
						p_links.append(entry_link)
						break
			
			# Return only those ones with something interesting
			if len(repos) == 0:
				self.logger.info(f'No identified repo for {entry_id}: (links {entry_links})')
			queries.append({
				'@id': entry_id,
				'entry_links': entry_links,
				'repos': repos
			})
		
		return queries
