#!/usr/bin/env python3

import configparser
import datetime
import re
import statistics
from typing import Any, List, Mapping, Optional, Tuple
import urllib

from .abstract import AbstractRepoMatcher, RepoMatcherException
from ..common import datetimeFromISOFormat

class GitHubRepoMatcher(AbstractRepoMatcher):
	# Constants
	GITHUB_KIND = 'github'

	GITHUB_HOST = 'github.com'
	GITHUB_IO_HOST = 'github.io'
	
	GITHUB_PATTERN = ':\/\/' + GITHUB_HOST + '\/([^"\'\/]+)\/([^"\'\/]+)'
	GITHUB_COMPILED_PATTERN = re.compile(GITHUB_PATTERN)

	GITHUB_ENDPOINT = 'https://' + GITHUB_HOST
	GITHUB_API_ENDPOINT = 'https://api.github.com/'
	
	GITHUB_API_V_HEADER = 'application/vnd.github.v3+json'

	def __init__(self, config: configparser.ConfigParser):
		super().__init__(config)
		
		self.githubRepoDataCache = dict()
		self.githubUserCache = dict()
		# $self->{'isoparser'} = DateTime::Format::ISO8601->new();
	
	@classmethod
	def kind(cls) -> str:
		return cls.GITHUB_KIND
	
	# This is an overriden method
	def _getCredentials(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
		gh_user = self.config.get(self.GITHUB_KIND, 'gh-user', fallback=None)
		gh_token = self.config.get(self.GITHUB_KIND, 'gh-token', fallback=None)
		
		return gh_user, gh_token, self.GITHUB_API_ENDPOINT
	
	def doesMatch(self, uriStr: str) -> Tuple[bool, Optional[str], Optional[str]]:
		isUri = False
		owner = None
		repo = None
		
		# Is an URI from GitHub?
		try:
			ghURI = urllib.parse.urlparse(uriStr)
			if ghURI.scheme is not None:
				isUri = True
				if ghURI.scheme.startswith('http') or ghURI.scheme.startswith('git'):
					host = ghURI.netloc
					path = ghURI.path.split('/')
					if len(path) > 0 and path[0] == '':
						path = path[1:]
					
					if host == self.GITHUB_HOST:
						if len(path) >= 2:
							owner, repo = path[0:2]
							
							# If it ends in \.git
							if repo.endswith('.git'):
								repo = repo[:-4]
					elif host.endswith(self.GITHUB_IO_HOST):
						if len(path) >= 1:
							owner = host[0:-len(self.GITHUB_IO_HOST)-1]
							
							if owner == 'htmlpreview':
								isUri, owner, repo = self.doesMatch(ghURI.query)
								return isUri, owner, repo
							elif len(path[0]) > 0:
								repo = path[0]
						
						if repo is None:
							# It is some kind of web
							req = urllib.request.Request(uriStr)
							try:
								with urllib.request.urlopen(req) as response:
									# TODO: guess encoding
									page = response.read().decode('iso-8859-15')
									matched = self.GITHUB_COMPILED_PATTERN.match(page)
									if matched is not None:
										owner = matched.group(1)
										repo = matched.group(2)
							except:
								pass
		except Exception as e:
			self.logger.exception('Failure in doesMatch')
		
		return isUri , owner, repo


	def getGitHubUser(self, username) -> Tuple[bool, Mapping]:
		userData = self.githubUserCache.get(username)
		userSuccess = False
		
		if userData is None:
			userUri = urllib.request.urljoin(
				self.GITHUB_API_ENDPOINT, 
				'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "users", username])),
			)
			
			userSuccess, userData = self.fetchJSON(userUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
			
			if isinstance(userData, list):
				userData = userData[0]
			
			# Record it in the user cache
			if userSuccess:
				self.githubUserCache[username] = userData
			
		else:
			userSuccess = True
		
		return userSuccess, userData

	def getRepoData(self, fullrepo: Mapping[str, Any]) -> Mapping[str, Any]:
		owner = fullrepo['workspace']
		repo = fullrepo['repo']
		
		lcOwner = owner.lower()
		lcRepo = repo.lower()
		
		p_githubRepoDataCache = self.githubRepoDataCache
		
		ans = p_githubRepoDataCache.setdefault(lcOwner, dict()).get(lcRepo)
		
		if ans is None:
			ans = dict()
			
			# These are all the URIs the program needs to fetch from GitHub
			
			self.logger.info(f" * Processing {owner} {repo}")
			repoUri = urllib.request.urljoin(
				self.GITHUB_API_ENDPOINT, 
				'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", owner, repo])),
			)
			repoSuccess, repoData = self.fetchJSON(repoUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
			
			minProcessing = not repoSuccess
			
			if repoSuccess:
				self.logger.info(f"\t- Extended processing {owner} {repo}")
				# The real repo and/or owner could be different from the registered one
				realOwner = None
				lcRealOwner = None
				realRepo = None
				lcRealRepo = None
				followProcessing = True
				
				realOwner = repoData.get('owner', dict()).get('login')
				if realOwner is not None:
					lcRealOwner = realOwner.lower()
					
					p_githubRepoDataCache.setdefault(lcRealOwner, dict())
					
					realRepo = repoData.get('name')
					if realRepo is not None:
						lcRealRepo = realRepo.lower()
				
				if realRepo is not None:
					if lcRealOwner != lcOwner or lcRealRepo != lcRepo:
						if lcRealRepo in p_githubRepoDataCache[lcRealOwner]:
							# Nothing more to do, work previously done
							ans = p_githubRepoDataCache[lcRealOwner][lcRealRepo]
							p_githubRepoDataCache[lcOwner][lcRepo] = ans
							
							followProcessing = False
						else:
							# Two for one bargain!
							p_githubRepoDataCache[lcRealOwner][lcRealRepo] = ans
							p_githubRepoDataCache[lcOwner][lcRepo] = ans
					else:
						p_githubRepoDataCache[lcRealOwner][lcRealRepo] = ans
				else:
					# Nothing more to do here, min processing
					followProcessing = False
					minProcessing = True
				
				if followProcessing:
					clone_url = repoData.get('clone_url')
					if clone_url is not None:
						ans['owner'] = realOwner
						ans['workspace'] = realOwner
						ans['repo'] = realRepo
						
						ans['numForks'] = repoData['forks_count']
						ans['numWatchers'] = repoData['watchers_count']
						ans['creationDate'] = repoData['created_at']
						ans['lastUpdate'] = repoData['updated_at']
						ans['lastPush'] = repoData['pushed_at']
						
						ans['vcs_type'] = 'git'
						# sourcecode repository
						ans['vcs_uri'] = clone_url
						# sourcecode
						ans['source_uri'] = clone_url
						# sourcecode download registration
						ans['source_isDownloadRegistered'] = False
						# sourcecode anonymous access
						ans['source_hasAnonymousAccess'] = True
						# sourcecode repository browse
						ans['source_isRepoBrowsable'] = True
						# Source_code repository version controlled
						ans['source_hasVcsRepo'] = True
						
						# TO IMPROVE
						# Source code repository accessible
						ans['source_isRepoAccessible'] = True
					
					# website
					homepage = repoData.get('homepage')
					html_url = repoData.get('html_url')
					if isinstance(homepage, str) and len(homepage) > 0:
						ans['has_website'] = True
						ans['website'] = homepage
					elif isinstance(html_url, str) and len(html_url) > 0:
						ans['has_website'] = True
						ans['website'] = html_url
					
					description = repoData.get('description')
					if description is not None:
						# canonical:description
						ans['desc'] = description
						# canonical:concept
						ans['concept'] = description
					
					# canonical|version:HELP
					ans['has_help'] = repoData.get('has_wiki', False) or repoData.get('has_pages', False)
					# canonical|version:Tutorial
					ans['has_tutorial'] = repoData.get('has_wiki', False) or repoData.get('has_pages', False)
					
					if repoData.get('has_issues', False):
						# issue tracker
						ans['has_issue_tracker'] = True

						issuesUri = urllib.request.urljoin(
							self.GITHUB_API_ENDPOINT, 
							'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", realOwner, realRepo, 'issues'])) +
							'?' + urllib.parse.urlencode({'state': 'all'}),
						)
						
						issuesSuccess, issuesData = self.fetchJSON(issuesUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
						
						# It gathers the date of the last issue, and the date of the last update
						if issuesSuccess:
							
							numOpen = 0
							numClosed = 0
							lastClosed = None
							lastUpdated = None
							lastCreated = None
							
							minDuration = None
							maxDuration = None
							totalSeconds = list()
							
							for issue in issuesData:
								if issue['state'] == 'closed':
									numClosed += 1
									closedDt = datetimeFromISOFormat(issue['closed_at'])
									createdDt = datetimeFromISOFormat(issue['created_at'])
									duration = closedDt - createdDt
									
									totalSeconds.append(duration.total_seconds())
									if not isinstance(minDuration, datetime.timedelta) or duration < minDuration:
										minDuration = duration
									
									if not isinstance(maxDuration, datetime.timedelta) or maxDuration > duration:
										maxDuration = duration
								else:
									numOpen += 1
								
								issueCreated = issue.get('created_at')
								if (issueCreated is not None) and ((lastCreated is None) or lastCreated < issueCreated):
									lastCreated = issueCreated
								
								issueUpdated = issue.get('updated_at')
								if (issueUpdated is not None) and ((lastUpdated is None) or lastUpdated < issueUpdated):
									lastUpdated = issueUpdated
								
								issueClosed = issue.get('closed_at')
								if (issueClosed is not None) and ((lastClosed is None) or lastClosed < issueClosed):
									lastClosed = issueClosed
							
							issues = {
								'total': len(issuesData),
								'open': numOpen,
								'closed': numClosed,
								'lastClosedDate': lastClosed,
								'lastUpdatedDate': lastUpdated,
								'lastCreatedDate': lastCreated
							}
							if minDuration is not None:
								issues['minDuration'] = minDuration
								issues['maxDuration'] = maxDuration
								
								meanSeconds = round(statistics.mean(totalSeconds))
								medianSeconds = round(statistics.median(totalSeconds))
								issues['meanDuration'] = datetime.timedelta(seconds=meanSeconds)
								issues['medianDuration'] = datetime.timedelta(seconds=medianSeconds)
							
							ans['issues'] = issues
					
					# All the gathered version tags go to this array
					versions = list()
					
					releasesUri = urllib.request.urljoin(
						self.GITHUB_API_ENDPOINT, 
						'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", realOwner, realRepo, 'releases']))
					)
					releasesSuccess, releasesData = self.fetchJSON(releasesUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
					if releasesSuccess:
						# Versions
						for release in releasesData:
							versions.append(release['tag_name'])
							
							release_assets = release.get('assets')
							if isinstance(release_assets, list):
								binaries = ans.setdefault('binaries', list())
								for asset in release_assets:
									p_b = {
										'creationDate': asset['created_at'],
										'numDownloads': asset['download_count'],
										'binary_uri': asset['browser_download_url'],
										'binary_isDistributable': True,
										'binary_isDownloadRegistered': False,
									}
									# binary distribution
									if ans.get('binary_uri') is None:
										ans['binary_uri'] = asset['browser_download_url']
										# binary distribution freeness
										ans['binary_isDistributable'] = True
										# binaries download registration
										ans['binary_isDownloadRegistered'] = False
									
									binaries.append(p_b)
					
					
					tagsUri = urllib.request.urljoin(
						self.GITHUB_API_ENDPOINT, 
						'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", realOwner, realRepo, 'tags']))
					)
					tagsSuccess, tagsData = self.fetchJSON(tagsUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
					if tagsSuccess:
						# Tags treated as versions
						vercheck = set(versions)
						for tag in tagsData:
							tag_name = tag['name']
							if (tag_name is not None) and (tag_name not in vercheck):
								versions.append(tag_name)
					
					langsUri = urllib.request.urljoin(
						self.GITHUB_API_ENDPOINT, 
						'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", realOwner, realRepo, 'languages']))
					)
					langsSuccess, langsData = self.fetchJSON(langsUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
					# Using only the first element
					if isinstance(langsData, list):
						langsData = langsData[0]
					if langsSuccess:
						ans['languages'] = list(langsData.keys())
						
						interpreted = False
						compiled = False
						for lang in ans['languages']:
							lcLang = lang.lower()
							if lcLang in self.recognizedInterpretedLanguages:
								interpreted = True
							if lcLang in self.recognizedCompiledLanguages:
								compiled = True
						ans['has_interpretedLanguages'] = interpreted
						ans['has_compiledLanguages'] = compiled
						
						# 'type'
						for lang, build_tool in self.recognizedBuildSystemsByLang.items():
							if lang in langsData:
								ans['tool_buildSystem'] = build_tool
								break
					
					contributors = list()
					
					contrUri = urllib.request.urljoin(
						self.GITHUB_API_ENDPOINT, 
						'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", realOwner, realRepo, 'contributors']))
					)
					contrSuccess, contrData = self.fetchJSON(contrUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
					if contrSuccess:
						# credits
						for contributor in contrData:
							contributor_login = contributor['login']
							user_obtained, p_contrib = self.getGitHubUser(contributor_login)

							contribOpEB = {
								'username': contributor_login,
							}
							
							for key in ('name','company','location','email'):
								valkey = p_contrib.get(key)
								if valkey is not None:
									contribOpEB[key] = valkey
							contributors.append(contribOpEB)
					
					
					# sourcecode freeness. The default license is the owner itself
					ans['source_isFree'] = False
					ans['has_license'] = repoData.get('license') is not None
					
					if ans['has_license']:
						licUri = urllib.request.urljoin(
							self.GITHUB_API_ENDPOINT, 
							'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", realOwner, realRepo, 'license']))
						)
						licSuccess, licData = self.fetchJSON(licUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
						if isinstance(licData, list):
							licData = licData[0]
						if licSuccess:
							# sourcecode freeness
							ans['source_isFree'] = True
							html_link = licData.get('_links', dict()).get('html')
							if html_link is not None:
								# project license
								ans['tool_license_uri'] = html_link
								# sourcecode license
								ans['source_license_uri'] = html_link
							
							lic = licData.get('license', dict())
							spdx_id = lic.get('spdx_id')
							lic_key = lic.get('key')
							lic_name = lic.get('name')
							if spdx_id is not None:
								ans['source_license'] = spdx_id
							elif lic_key is not None:
								ans['source_license'] = lic_key
							elif lic_name is not None:
								ans['source_license'] = lic_name
							
							# TO IMPROVE
							if 'featured' in lic:
								# 'open source'
								ans['is_opensource'] = True
								# 'OSI'
								ans['is_OSI'] = True
					
					# Looking for a README and other elements
					commitUri = urllib.request.urljoin(
						self.GITHUB_API_ENDPOINT, 
						'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", realOwner, realRepo, 'commits']))
					)
					commitSuccess, commitData = self.fetchJSON(commitUri, p_acceptHeaders=self.GITHUB_API_V_HEADER, numIter=1)
					if commitSuccess and len(commitData) > 0:
						treeUri = commitData[0]['commit']['tree']['url']
						
						treeSuccess, treeData = self.fetchJSON(treeUri, p_acceptHeaders=self.GITHUB_API_V_HEADER)
						if isinstance(treeData, list):
							treeData = treeData[0]
						if treeSuccess:
							for elem in treeData['tree']:
								# Finding a readme like
								if elem['path'].lower().startswith('readme'):
									ans['readmeFile'] = elem['path']
									break
					
					#my $pullsUri = URI->new(GITHUB_API_ENDPOINT);
					#$pullsUri->path_segments("","repos",$realOwner,$realRepo,'pulls');
					#my($pullsSuccess,$pullsData) = $self->fetchJSON($pullsUri,$gh_user,$gh_token,GITHUB_API_V_HEADER);
					#if($pullsSuccess) {
					#	use Data::Dumper;
					#	print Dumper($pullsData),"\n";
					#}
					#
					#
					
					# Versions
					if len(versions) > 0:
						ans['tool_versions'] = versions
					# credits
					if len(contributors) > 0:
						ans['tool_developers'] = contributors
			
			# These are failed, strange cases
			if minProcessing:
				p_githubRepoDataCache[lcOwner][lcRepo] = ans
				
				# Alternate way
				clone_uri = urllib.request.urljoin(
					self.GITHUB_API_ENDPOINT, 
					'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["", "repos", owner, repo])),
				)
				ans['vcs_type'] = 'git'
				# sourcecode repository
				ans['vcs_uri'] = clone_uri
				
				ans['owner'] = owner
				ans['workspace'] = owner
				ans['repo'] = repo
		
		return ans
