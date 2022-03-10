#!/usr/bin/env python3

import configparser
import datetime
import re
import statistics
from typing import Any, List, Mapping, Optional, Tuple
import urllib

from .abstract import AbstractRepoMatcher, RepoMatcherException
from ..common import datetimeFromISOFormat

class BitBucketRepoMatcher(AbstractRepoMatcher):
	# Documentation: https://developer.atlassian.com/server/bitbucket/reference/rest-api/

	# Constants
	BITBUCKET_KIND = 'bitbucket'

	BITBUCKET_HOST = 'bitbucket.org'
	BITBUCKET_IO_HOST = 'bitbucket.io'
	BITBUCKET_PAGELEN = 'pagelen'
	
	BITBUCKET_PATTERN = ':\/\/(?:[^@\/]+@)?' + BITBUCKET_HOST + '\/([^"\'\/]+)\/([^"\'\/]+)(?:\.git)?'
	BITBUCKET_COMPILED_PATTERN = re.compile(BITBUCKET_PATTERN)
	
	EMAIL_COMMIT_PATTERN = r'^([^\<]+) \<([^\>]+)\>$'
	EMAIL_COMMIT_COMPILED_PATTERN = re.compile(EMAIL_COMMIT_PATTERN)

	BITBUCKET_ENDPOINT = 'https://' + BITBUCKET_HOST + '/'
	BITBUCKET_API_ENDPOINT = 'https://api.' + BITBUCKET_HOST + '/2.0/'
	BITBUCKET_API_ACCEPT_HEADER = '*/*;q=0.2,application/json'

	BITBUCKET_IO_HOST = 'bitbucket.io'
	
	def __init__(self, config: configparser.ConfigParser):
		super().__init__(config)
		
		self.bitbucketRepoDataCache = dict()
		self.bitbucketUserCache = dict()
		
		# Needed to set up the size of the requested batches of data
		self.pageSize = config.getint(self.BITBUCKET_KIND, 'page-size', fallback=100)
	
	@classmethod
	def kind(cls) -> str:
		return cls.BITBUCKET_KIND
	
	# This is an overriden method
	def _getCredentials(self) -> Tuple[Optional[str], Optional[str], Optional[str]]:
		bb_user = self.config.get(self.BITBUCKET_KIND, 'user-slug', fallback=None)
		bb_token = self.config.get(self.BITBUCKET_KIND, 'user-token', fallback=None)
		
		return bb_user, bb_token, self.BITBUCKET_API_ENDPOINT

	def doesMatch(self, uriStr: str) -> Tuple[bool, Optional[str], Optional[str]]:
		isUri = False
		workspace = None
		repoSlug = None
		
		# Is an URI from BitBucket?
		try:
			bbURI = urllib.parse.urlparse(uriStr)
			if bbURI.scheme is not None:
				isUri = True
				if bbURI.scheme.startswith('http'):
					host = bbURI.netloc
					path = bbURI.path.split('/')
					if len(path) > 0 and path[0] == '':
						path = path[1:]
					
					if host == self.BITBUCKET_HOST:
						if len(path) >= 2:
							workspace, repoSlug = path[0:2]
							
							# If it ends in \.git
							if repoSlug.endswith('.git'):
								repoSlug = repoSlug[:-4]
					elif host.endswith(self.BITBUCKET_HOST):
						if len(path) >= 1:
							workspace = host[0:-len(self.BITBUCKET_HOST)-1]
							
							if workspace == 'htmlpreview':
								isUri, workspace, repoSlug = self.doesMatch(bbURI.query)
								return isUri, workspace, repoSlug
							elif len(path[0]) > 0:
								repoSlug = path[0]
						
						if repoSlug is None:
							# It is some kind of web
							req = urllib.request.Request(uriStr)
							try:
								with urllib.request.urlopen(req) as response:
									# TODO: guess encoding
									page = response.read().decode('iso-8859-15')
									matched = self.BITBUCKET_COMPILED_PATTERN.match(page)
									if matched is not None:
										workspace = matched.group(1)
										repoSlug = matched.group(2)
							except:
								pass
		except Exception as e:
			self.logger.exception('Failure in doesMatch')
		
		return isUri,workspace, repoSlug


	# https://developer.atlassian.com/bitbucket/api/2/reference/resource/users/%7Busername%7D
	def getBitBucketUser(self, username: str, endpoint: Optional[str] = None) -> Tuple[bool, List[Mapping]]:
		if endpoint is None:
			endpoint = self.BITBUCKET_API_ENDPOINT
		
		userData = self.bitbucketUserCache.get(username)
		userSuccess = False
		
		if userData is not None:
			userSuccess = True
		else:
			userUri = urllib.request.urljoin(
				self.BITBUCKET_API_ENDPOINT,
				'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["users", username])),
			)
			
			userSuccess, userData = self.fetchJSON(userUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
			
			if isinstance(userData, list):
				userData = userData[0]
			
			# Record it in the user cache
			if userSuccess:
				self.bitbucketUserCache[username] = userData
		
		return userSuccess, userData

	def getRepoData(self, fullrepo: Mapping[str, Any]) -> Mapping[str, Any]:
		workspace = fullrepo['owner']
		repoSlug = fullrepo['repo']
		
		lcWorkspace = workspace.lower()
		lcRepoSlug = repoSlug.lower()
		
		p_bitbucketRepoDataCache = self.bitbucketRepoDataCache
		
		p_bitbucketRepoDataCache.setdefault(lcWorkspace, dict())
		
		ans = p_bitbucketRepoDataCache.setdefault(lcWorkspace, dict()).get(lcRepoSlug)
		
		if ans is None:
			ans = dict()
			
			self.logger.info(f" * Processing {workspace} {repoSlug}")
			
			# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D
			repoUri = urllib.request.urljoin(
				self.BITBUCKET_API_ENDPOINT,
				'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["repositories", workspace, repoSlug])),
			)
			repoSuccess, repoData = self.fetchJSON(repoUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
			
			minProcessing = not repoSuccess
						
			if repoSuccess:
				self.logger.info(f"\t- Extended processing {workspace} {repoSlug}")
				# The real repo and/or owner could be different from the registered one
				realOwner = None
				lcRealOwner = None
				realWorkspace = None
				realRepoSlug = None
				lcRealRepoSlug = None
				followProcessing = True
				
				ownerH = repoData.get('owner')
				if isinstance(ownerH, dict):
					userKey = None
					for key in ('nickname','username'):
						if key in ownerH:
							userKey = key
							break
					
					if userKey is not None:
						realOwner = ownerH[userKey]
						lcRealOwner = realOwner.lower()
						
						p_bitbucketRepoDataCache.setdefault(lcRealOwner, dict())
						
						realRepoSlug = repoData.get('slug')
						if realRepoSlug is not None:
							lcRealRepoSlug = realRepoSlug.lower()
						
						realWorkspace = ownerH.get('uuid')
						if realWorkspace is not None:
							p_bitbucketRepoDataCache[realWorkspace] = p_bitbucketRepoDataCache[lcRealOwner]
					else:
						import json
						self.logger.error(f"ASSERT: Tell the developer about this entry ({workspace} {repoSlug})\n\n{json.dumps(ownerH, sort_keys=True, indent=4)}")
				
				if realRepoSlug is not None:
					if lcRealOwner != lcWorkspace or lcRealRepoSlug != lcRepoSlug:
						if lcRealRepoSlug in p_bitbucketRepoDataCache[lcRealOwner]:
							# Nothing more to do, work previously done
							ans = p_bitbucketRepoDataCache[lcRealOwner][lcRealRepoSlug]
							p_bitbucketRepoDataCache[lcWorkspace][lcRepoSlug] = ans
							
							followProcessing = False
						else:
							# Two for one bargain!
							p_bitbucketRepoDataCache[lcRealOwner][lcRealRepoSlug] = ans
							p_bitbucketRepoDataCache[lcWorkspace][lcRepoSlug] = ans
					else:
						p_bitbucketRepoDataCache[lcRealOwner][lcRealRepoSlug] =  ans
				else:
					# Nothing more to do here, min processing
					followProcessing = False
					minProcessing = True
				
				linksH = repoData.get('links')
				if followProcessing and isinstance(linksH, dict):
					ans['owner'] = realOwner
					ans['workspace'] = realWorkspace
					ans['repo'] = realRepoSlug
					
					ans['creationDate'] = repoData.get('created_on')
					ans['lastUpdate'] = repoData.get('updated_on')
					
					ans['vcs_type'] = repoData['scm']
					# sourcecode repository
					
					cloneA = linksH.get('clone')
					if isinstance(cloneA, list):
						for cloneP in cloneA:
							cloneP_href = cloneP.get('href')
							if (cloneP_href is not None) and cloneP.get('name') == 'https':
								# We have to remove the user name from URI
								# in case we have been issuing authenticated queries
								cP = urllib.parse.urlparse(cloneP_href)
								cP_usermark = cP.netloc.find('@')
								if cP_usermark != -1:
									cP_netloc = cP.netloc[cP_usermark+1:]
									cloneP_href = urllib.parse.urlunparse((
										cP[0],
										cP_netloc,
										cP[2],
										cP[3],
										cP[4],
										cP[5]
									))
								ans['vcs_uri'] = cloneP_href
								# sourcecode
								ans['source_uri'] = cloneP_href
								break
					
					# sourcecode download registration
					ans['source_isDownloadRegistered'] = False
					# sourcecode anonymous access
					ans['source_hasAnonymousAccess'] = not repoData.get('is_private', True)
					# sourcecode repository browse
					ans['source_isRepoBrowsable'] = True
					# Source_code repository version controlled
					ans['source_hasVcsRepo'] = True
					
					# TO IMPROVE
					# Source code repository accessible
					ans['source_isRepoAccessible'] = True
					
					# website
					website = repoData.get('website')
					if (website is None) or len(website) == 0:
						website = linksH.get('html', dict()).get('href')
						
					if isinstance(website, str) and len(website) > 0:
						ans['has_website'] = True
						ans['website'] = website
					
					description = repoData.get('description')
					if isinstance(description, str) and len(description) > 0:
						# canonical:description
						ans['desc'] = description
						# canonical:concept
						ans['concept'] = description
					
					# canonical|version:HELP
					ans['has_help'] = repoData.get('has_wiki', False)
					# canonical|version:Tutorial
					ans['has_tutorial'] = repoData.get('has_wiki', False)
					
					if repoData.get('has_issues', False):
						# issue tracker
						ans['has_issue_tracker'] = True

						issuesUri = None
						
						issuesH = linksH.get('issues')
						if isinstance(issuesH, dict):
							issuesUri = issuesH.get('href')
						else:
							# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/issues
							issuesUri = urllib.request.urljoin(
								self.BITBUCKET_API_ENDPOINT,
								'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , ["repositories", realOwner, realRepoSlug, 'issues'])),
							)
							issuesSuccess, issuesData = self.fetchJSON(repoUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
						issuesUri += '?' + urllib.parse.urlencode({self.BITBUCKET_PAGELEN: self.pageSize})
						
						# Now, accounting
						numOpen = 0
						numClosed = 0
						lastClosed = None
						lastUpdated = None
						lastCreated = None
						
						minDuration = None
						maxDuration = None
						totalSeconds = list()
						
						total = None
						while issuesUri is not None:
							issuesSuccess, issuesData = self.fetchJSON(issuesUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
							
							issuesUri = None
							# It gathers the date of the last issue, and the date of the last update
							if issuesSuccess:
								# First, check next iteration link
								issuesUri = issuesData.get('next')
								
								# This is redundant, but we are giving a change to record in course issues
								total = issuesData.get('size')
								
								for issue in issuesData.get('values', list()):
									issueCreated = issue.get('created_on')
									if (issueCreated is not None) and ((lastCreated is None) or lastCreated < issueCreated):
										lastCreated = issueCreated
									
									issueUpdated = issue.get('updated_on')
									if (issueUpdated is not None) and ((lastUpdated is None) or lastUpdated < issueUpdated):
										lastUpdated = issueUpdated
									if issue.get('state') in ('closed', 'resolved'):
										numClosed += 1
										closedDt = datetimeFromISOFormat(issue['updated_on'])
										createdDt = datetimeFromISOFormat(issue['created_on'])
										duration = closedDt - createdDt
										
										totalSeconds.append(duration.total_seconds())
										if not isinstance(minDuration, datetime.timedelta) or duration < minDuration:
											minDuration = duration
										
										if not isinstance(maxDuration, datetime.timedelta) or maxDuration > duration:
											maxDuration = duration
										
										issueUpdated = issue.get('updated_on')
										if (issueUpdated is not None) and ((lastUpdated is None) or lastUpdated < issueUpdated):
											lastUpdated = issueUpdated
										# We have to simulate the lastClosed based on the last update of those which are resolved or closed
										if (issueUpdated is not None) and ((lastClosed is None) or lastClosed < issueUpdated):
											lastClosed = issueUpdated
									else:
										numOpen += 1
						
						issues = {
							'total': total,
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
					
					# All the gathered versions and tags go to this array
					versions = list()
					releasesUri = linksH.get('downloads', dict()).get('href')
					if releasesUri is not None:
						# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/refs/tags
						#$releasesUri = URI->new(BITBUCKET_API_ENDPOINT);
						#$releasesUri->path_segments($tagsUri->path_segments(),"repositories",$realOwner,$realRepoSlug,'downloads');
						releasesUri += '?' + urllib.parse.urlencode({self.BITBUCKET_PAGELEN: self.pageSize})
						
						while releasesUri is not None:
							releasesSuccess, releasesData = self.fetchJSON(releasesUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
							releasesUri = None
							
							if releasesSuccess:
								# Versions
								releasesUri = releasesData.get('next')
								binaries = ans.setdefault('binaries', list())
								for release in releasesData.get('values', list()):
									versions.append(release['name'])
									
									binaryUri = release.get('links', dict()).get('self', dict()).get('href')
									
									p_b = {
										'binary_uri': binaryUri,
										'binary_isDistributable': True,
										'binary_isDownloadRegistered': False,
										'creationDate': release['created_on'],
										'numDownloads': release['downloads']
									}
									# binary distribution
									if 'binary_uri' not in ans:
										ans['binary_uri'] = binaryUri
										# binary distribution freeness
										ans['binary_isDistributable'] = True
										# binaries download registration
										ans['binary_isDownloadRegistered'] = False
									
									binaries.append(p_b)
					
					tagsUri = linksH.get('tags', dict()).get('href')
					if tagsUri is not None:
						# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/refs/tags
						#$tagsUri = URI->new(BITBUCKET_API_ENDPOINT);
						#$tagsUri->path_segments($tagsUri->path_segments(),"repositories",$realOwner,$realRepoSlug,'refs','tags');
						tagsUri += '?' + urllib.parse.urlencode({self.BITBUCKET_PAGELEN: self.pageSize})
						while tagsUri is not None:
							tagsSuccess, tagsData = self.fetchJSON(tagsUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
							tagsUri = None
							
							if tagsSuccess:
								# First, check next iteration link
								tagsUri = tagsData.get('next')
								
								# Tags treated as versions
								for tag in tagsData['values']:
									versions.append(tag['name'])
					
					language = repoData.get('language')
					if isinstance(language, str) and len(language) > 0:
						# This can be improved using external programs
						ans['languages'] = [ language ]
						
						interpreted = False
						compiled = False
						for lang in ans['languages']:
							lcLang = lang.lower()
							interpreted = interpreted or (lcLang in self.recognizedInterpretedLanguages)
							compiled = compiled or (lcLang in self.recognizedCompiledLanguages)
						ans['has_interpretedLanguages'] = interpreted
						ans['has_compiledLanguages'] = compiled
						
						# 'type'
						lcLanguage = language.lower()
						tool_buildSystem = self.recognizedBuildSystemsByLang.get(lcLanguage)
						if tool_buildSystem is not None:
							ans['tool_buildSystem'] = tool_buildSystem
					
					contributors = list()
					contrUri = linksH.get('commits', dict()).get('href')
					if contrUri is not None:
						# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/commits
						#my $contrUri = URI->new(BITBUCKET_API_ENDPOINT);
						#$contrUri->path_segments($contrUri->path_segments(),"repositories",$realOwner,$realRepoSlug,'commits');
						contrUri += '?' + urllib.parse.urlencode({self.BITBUCKET_PAGELEN: self.pageSize})
						
						contribSet = set()
						
						while contrUri is not None:
							contrSuccess, contrData = self.fetchJSON(contrUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
							contrUri = None
							
							if contrSuccess:
								contrUri = contrData.get('next')
								
								# credits
								for commit in contrData.get('values', list()):
									p_contrib = commit['author']

									contribOpEB = dict()
									
									username = None
									name = p_contrib.get('raw')
									email = None
									
									ematch = self.EMAIL_COMMIT_COMPILED_PATTERN.match(name)
									if ematch is not None:
										name = ematch.group(1)
										email = ematch.group(2)
									
									user = p_contrib.get('user')
									if isinstance(user, dict):
										username = user.get('nickname', user.get('username'))
										name = user.get('display_name')
									
									if name not in contribSet:
										p_contribOpEB = {
											'name': name
										}
										if email is not None:
											p_contribOpEB['email'] = email
										if username is not None:
											p_contribOpEB['username'] = username
										
										contributors.append(p_contribOpEB)
										contribSet.add(name)
					
					forksUri = linksH.get('forks', dict()).get('href')
					if forksUri is not None:
						# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/forks
						forksSuccess, forksData = self.fetchJSON(forksUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
						
						if forksSuccess:
							ans['numForks'] = forksData['size']
					
					watchersUri = linksH.get('watchers', dict()).get('href')
					if watchersUri is not None:
						# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/watchers
						watchersSuccess, watchersData = self.fetchJSON(watchersUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
						
						if watchersSuccess:
							ans['numWatchers'] = watchersData['size']
					
					branchesUri = linksH.get('branches', dict()).get('href')
					if branchesUri is not None:
						# https://developer.atlassian.com/bitbucket/api/2/reference/resource/repositories/%7Bworkspace%7D/%7Brepo_slug%7D/refs/branches
						branchesUri += '?' + urllib.parse.urlencode({self.BITBUCKET_PAGELEN: self.pageSize})
						
						# We read about all the branches, and we have a look at its head
						lastPush = ''
						branches = list()
						while branchesUri is not None:
							branchesSuccess, branchesData = self.fetchJSON(branchesUri, p_acceptHeaders=self.BITBUCKET_API_ACCEPT_HEADER)
							branchesUri = None
							
							if branchesSuccess:
								branchesUri = branchesData.get('next')
								
								for branch in branchesData.get('values', list()):
									p_br = {
										'name': branch['name']
									}
									branches.append(p_br)
									
									target_date = branch.get('target', dict()).get('date')
									if target_date is not None:
										p_br['lastPush'] = target_date
										if target_date >= lastPush:
											lastPush = target_date
						
						# At last
						ans['lastPush'] = lastPush
						ans['branches'] = branches
					
					
					# TBD with an external program (sloooooooooooooooow)
					## sourcecode freeness. The default license is the owner itself
					#$ans{'source_isFree'} = JSON->false;
					#$ans{'has_license'} = defined($repoData->{'license'}) ? JSON->true : JSON->false;
					#
					#if($ans{'has_license'}) {
					#	my $licUri = URI->new(GITHUB_API_ENDPOINT);
					#	$licUri->path_segments("","repos",$realOwner,$realRepoSlug,'license');
					#	my($licSuccess,$licData) = $self->fetchJSON($licUri,$bb_user,$bb_token);
					#	if($licSuccess) {
					#		# sourcecode freeness
					#		$ans{'source_isFree'} = JSON->true;
					#		if(exists($licData->{'_links'}{'html'})) {
					#			# project license
					#			$ans{'tool_license_uri'} = $licData->{'_links'}{'html'};
					#			# sourcecode license
					#			$ans{'source_license_uri'} = $licData->{'_links'}{'html'};
					#		}
					#		
					#		if(exists($licData->{'license'}{'spdx_id'}) && defined($licData->{'license'}{'spdx_id'})) {
					#			$ans{'source_license'} = $licData->{'license'}{'spdx_id'};
					#		} elsif(exists($licData->{'license'}{'key'}) && defined($licData->{'license'}{'key'})) {
					#			$ans{'source_license'} = $licData->{'license'}{'key'};
					#		} elsif(exists($licData->{'license'}{'name'}) && defined($licData->{'license'}{'name'})) {
					#			$ans{'source_license'} = $licData->{'license'}{'name'};
					#		}
					#		
					#		# TO IMPROVE
					#		if($licData->{'license'}{'featured'}) {
					#			# 'open source'
					#			$ans{'is_opensource'} = JSON->true;
					#			# 'OSI'
					#			$ans{'is_OSI'} = JSON->true;
					#		}
					#	}
					#}
					
					# TBD (using command line tools?)
					## Looking for a README and other elements
					#my $commitUri = URI->new(GITHUB_API_ENDPOINT);
					#$commitUri->path_segments("","repos",$realOwner,$realRepoSlug,'commits');
					#my($commitSuccess,$commitData) = $self->fetchJSON($commitUri,$bb_user,$bb_token,1);
					#if($commitSuccess && scalar(@{$commitData}) > 0) {
					#	my $treeUri = $commitData->[0]{'commit'}{'tree'}{'url'};
					#	
					#	my($treeSuccess,$treeData) = $self->fetchJSON($treeUri,$bb_user,$bb_token);
					#	if($treeSuccess) {
					#		foreach my $elem (@{$treeData->{'tree'}}) {
					#			if(index(lc($elem->{'path'}),'readme') == 0) {
					#				$ans{'readmeFile'} = $elem->{'path'};
					#				last;
					#			}
					#		}
					#	}
					#}
					
					# Versions
					if len(versions) > 0:
						ans['tool_versions'] = versions
					# credits
					if len(contributors) > 0:
						ans['tool_developers'] = contributors
			
			# These are failed, strange cases
			if minProcessing:
				p_bitbucketRepoDataCache[lcWorkspace][lcRepoSlug] = ans
				
				# Alternate way
				clone_uri = urllib.request.urljoin(
					self.BITBUCKET_ENDPOINT,
					'/'.join(map(lambda p: urllib.parse.quote(p, safe='') , [workspace, repoSlug])),
				)
				
				# TODO: slow check whether it is git or mercurial through command line
				ans['vcs_type'] = 'git'
				# sourcecode repository
				ans['vcs_uri'] = clone_uri
				
				ans['owner'] = workspace
				ans['workspace'] = workspace
				ans['repo'] = repoSlug
		
		return ans
