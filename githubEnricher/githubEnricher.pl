#!/usr/bin/perl

use v5.10.1;
use strict;
use warnings 'all';

use FindBin;
use lib File::Spec->catfile($FindBin::Bin,'libs');

use OpenEBenchQueries;

use Carp;
use Config::IniFiles;


use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;
use Scalar::Util qw(blessed);
use Getopt::Long;

use File::Path qw();
use File::Spec;

use constant GITHUB_HOST => 'github.com';
use constant GITHUB_ENDPOINT => 'https://'.GITHUB_HOST;
use constant GITHUB_API_ENDPOINT => 'https://api.github.com/';

use constant GITHUB_IO_HOST => 'github.io';

use constant GITHUB_API_V_HEADER => 'application/vnd.github.v3+json';
# KeyOrder

my @TabKeyOrder = (
	['tool_id','Tool id'],
	['vcs_type','Repository Type'],
	['vcs_uri','sourcecode repository'],
	['source_uri','sourcecode'],
	['source_isFree','sourcecode freeness'],
	['source_isDownloadRegistered','sourcecode download registration'],
	['source_hasAnonymousAccess','sourcecode anonymous access'],
	['source_isRepoBrowsable','sourcecode repository browse'],
	['source_hasVcsRepo','Source_code repository version controlled'],
	['source_isRepoAccessible','Source code repository accessible'],
	
	['has_website','website'],
	['website','website url'],
	['desc','canonical:description'],
	['concept','canonical:concept'],
	['has_help','canonical|version:HELP'],
	['has_tutorial','canonical|version:Tutorial'],
	['has_issue_tracker','issue tracker'],
	
	['binary_uri','binary distribution'],
	['binary_isDistributable','binary distribution freeness'],
	['binary_isDownloadRegistered','binaries download registration'],
	['tool_versions','Versions'],
	
	['tool_buildSystem','type'],
	
	['tool_license_uri','project license'],
	['source_license_uri','sourcecode license'],
	['source_license','sourcecode license name'],
	['is_opensource','open source'],
	['is_OSI','OSI'],
	
	# This must be the last
	['tool_developers','credits']
);

sub fetchJSON($;$$$$) {
	my($bUri,$ua,$user,$token,$p_acceptHeaders) = @_;
	
	$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
	my $bUriStr = (blessed($bUri) && $bUri->can('as_string')) ? $bUri->as_string() : $bUri;
	my $req = HTTP::Request->new('GET' => $bUriStr);
	if(defined($p_acceptHeaders)) {
		my $headers = $req->headers();
		$headers->push_header(Accept => $p_acceptHeaders);
	}

	$req->authorization_basic($user,$token)  if(defined($user));
	
	sleep(1);
	my $response = $ua->request($req);
	
	my $bData = undef;
	if($response->is_success()) {
		eval {
			$bData = JSON::MaybeXS->new()->decode($response->decoded_content);
			#$bData = JSON->new()->utf8(1)->decode($response->decoded_content);
		};
		
		if($@) {
			print STDERR "ERROR: JSON parsing error: $@\n";
		}
	} else {
		print STDERR "ERROR: kicked out $bUri : ".$response->status_line()."\n";
	}
	
	return ($response->is_success(),$bData);
}

{
	my %gitHubUserCache = ();

	sub getGitHubUser($;$$$) {
		my($username,$ua,$user,$token) = @_;
		
		if(exists($gitHubUserCache{$username})) {
			return (1,$gitHubUserCache{$username});
		} else {
			$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
			
			my $userUri = URI->new(GITHUB_API_ENDPOINT);
			$userUri->path_segments("","users",$username);
			
			my($userSuccess,$userData) = fetchJSON($userUri,$ua,$user,$token,GITHUB_API_V_HEADER);
			
			$gitHubUserCache{$username} = $userData  if($userSuccess);
			
			return ($userSuccess,$userData);
		}
	}
}

{
	my %githubRepoDataCache = ();
	
	sub getGitHubRepoData(\%$$;$) {
		my($fullrepo,$gh_user,$gh_token,$ua) = @_;
		
		my($owner,$repo) = @{$fullrepo}{('owner','repo')};
		$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
		
		$githubRepoDataCache{$owner} = {}  unless(exists($githubRepoDataCache{$owner}));
		
		unless(exists($githubRepoDataCache{$owner}{$repo})) {
			my %ans = ();
			$githubRepoDataCache{$owner}{$repo} = \%ans;
			
			# These are all the URIs the program needs to fetch from GitHub
			
			my $repoUri = URI->new(GITHUB_API_ENDPOINT);
			$repoUri->path_segments("","repos",$owner,$repo);
			my($repoSuccess,$repoData) = fetchJSON($repoUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			if($repoSuccess) {
				if(exists($repoData->{'clone_url'}) && defined($repoData->{'clone_url'})) {
					$ans{'vcs_type'} = 'git';
					# sourcecode repository
					$ans{'vcs_uri'} = $repoData->{'clone_url'};
					# sourcecode
					$ans{'source_uri'} = $repoData->{'clone_url'};
					# sourcecode freeness
					$ans{'source_isFree'} = JSON->true;
					# sourcecode download registration
					$ans{'source_isDownloadRegistered'} = JSON->false;
					# sourcecode anonymous access
					$ans{'source_hasAnonymousAccess'} = JSON->true;
					# sourcecode repository browse
					$ans{'source_isRepoBrowsable'} = JSON->true;
					# Source_code repository version controlled
					$ans{'source_hasVcsRepo'} = JSON->true;
					
					# TO IMPROVE
					# Source code repository accessible
					$ans{'source_isRepoAccessible'} = JSON->true;
				}
				
				# website
				if(exists($repoData->{'homepage'}) && defined($repoData->{'homepage'}) && length($repoData->{'homepage'}) > 0) {
					$ans{'has_website'} = JSON->true;
					$ans{'website'} = $repoData->{'homepage'};
				} elsif(exists($repoData->{'html_url'}) && defined($repoData->{'html_url'}) && length($repoData->{'html_url'}) > 0) {
					$ans{'has_website'} = JSON->true;
					$ans{'website'} = $repoData->{'html_url'};
				}
				
				if(exists($repoData->{'description'}) && defined($repoData->{'description'})) {
					# canonical:description
					$ans{'desc'} = $repoData->{'description'};
					# canonical:concept
					$ans{'concept'} = $repoData->{'description'};
				}
				
				# canonical|version:HELP
				$ans{'has_help'} = ((exists($repoData->{'has_wiki'}) && $repoData->{'has_wiki'}) || (exists($repoData->{'has_pages'}) && $repoData->{'has_pages'}))? JSON->true : JSON->false;
				# canonical|version:Tutorial
				$ans{'has_tutorial'} = ((exists($repoData->{'has_wiki'}) && $repoData->{'has_wiki'}) || (exists($repoData->{'has_pages'}) && $repoData->{'has_pages'})) ? JSON->true : JSON->false;
				
				if($repoData->{'has_issues'}) {
					# issue tracker
					$ans{'has_issue_tracker'} = JSON->true;
				}
			} else {
				# Alternate way
				my $clone_uri = URI->new(GITHUB_ENDPOINT);
				$clone_uri->path_segments('',$owner,$repo);
				$ans{'vcs_type'} = 'git';
				# sourcecode repository
				$ans{'vcs_uri'} = $clone_uri->as_string();
			}
			
			# All the gathered version tags go to this array
			my @versions = ();
			
			my $releasesUri = URI->new(GITHUB_API_ENDPOINT);
			$releasesUri->path_segments("","repos",$owner,$repo,'releases');
			my($releasesSuccess,$releasesData) = fetchJSON($releasesUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			if($releasesSuccess) {
				# Versions
				foreach my $release (@{$releasesData}) {
					push(@versions,$release->{'tag_name'});
					
					if(exists($release->{'assets'}) && !exists($ans{'binary distribution'})) {
						foreach my $asset (@{$release->{'assets'}}) {
							# binary distribution
							$ans{'binary_uri'} = $asset->{'browser_download_url'};
							# binary distribution freeness
							$ans{'binary_isDistributable'} = JSON->true;
							# binaries download registration
							$ans{'binary_isDownloadRegistered'} = JSON->false;
							last;
						}
					}
				}
			}
			
			
			my $tagsUri = URI->new(GITHUB_API_ENDPOINT);
			$tagsUri->path_segments("","repos",$owner,$repo,'tags');
			my($tagsSuccess,$tagsData) = fetchJSON($tagsUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			if($tagsSuccess) {
				# Versions
				foreach my $tag (@{$tagsData}) {
					push(@versions,$tag->{'name'});
				}
			}
			
			my %recognizedBuildSystemsByLang = (
				'Makefile'	=>	'make'
			);
			
			my $langsUri = URI->new(GITHUB_API_ENDPOINT);
			$langsUri->path_segments("","repos",$owner,$repo,'languages');
			my($langsSuccess,$langsData) = fetchJSON($langsUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			if($langsSuccess) {
				# 'type'
				foreach my $lang (keys(%recognizedBuildSystemsByLang)) {
					if(exists($langsData->{$lang})) {
						$ans{'tool_buildSystem'} = $recognizedBuildSystemsByLang{$lang};
						last;
					}
				}
				
			}
			
			my @contributors = ();
			
			my $contrUri = URI->new(GITHUB_API_ENDPOINT);
			$contrUri->path_segments("","repos",$owner,$repo,'contributors');
			my($contrSuccess,$contrData) = fetchJSON($contrUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			if($contrSuccess) {
				# credits
				foreach my $contributor (@{$contrData}) {
					my $p_contrib = getGitHubUser($contributor->{'login'},$ua,$gh_user,$gh_token);

					my %contribOpEB = (
						'username'	=> $contributor->{'login'}
					);
					
					for my $key ( ('name','company','location','email') ) {
						if(exists($p_contrib->{$key}) && defined($p_contrib->{$key})) {
							$contribOpEB{$key} = $p_contrib->{$key};
						}
					}
					push(@contributors,\%contribOpEB);
				}
			}
			
			
			my $licUri = URI->new(GITHUB_API_ENDPOINT);
			$licUri->path_segments("","repos",$owner,$repo,'license');
			my($licSuccess,$licData) = fetchJSON($licUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			if($licSuccess) {
				if(exists($licData->{'_links'}{'html'})) {
					# project license
					$ans{'tool_license_uri'} = $licData->{'_links'}{'html'};
					# sourcecode license
					$ans{'source_license_uri'} = $licData->{'_links'}{'html'};
				}
				
				if(exists($licData->{'license'}{'spdx_id'}) && defined($licData->{'license'}{'spdx_id'})) {
					$ans{'source_license'} = $licData->{'license'}{'spdx_id'};
				} elsif(exists($licData->{'license'}{'key'}) && defined($licData->{'license'}{'key'})) {
					$ans{'source_license'} = $licData->{'license'}{'key'};
				} elsif(exists($licData->{'license'}{'name'}) && defined($licData->{'license'}{'name'})) {
					$ans{'source_license'} = $licData->{'license'}{'name'};
				}
				
				# TO IMPROVE
				if($licData->{'license'}{'featured'}) {
					# 'open source'
					$ans{'is_opensource'} = JSON->true;
					# 'OSI'
					$ans{'is_OSI'} = JSON->true;
				}
			}
			
			
			#my $pullsUri = URI->new(GITHUB_API_ENDPOINT);
			#$pullsUri->path_segments("","repos",$owner,$repo,'pulls');
			#my($pullsSuccess,$pullsData) = fetchJSON($pullsUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			#if($pullsSuccess) {
			#	use Data::Dumper;
			#	print Dumper($pullsData),"\n";
			#}
			#
			#
			#my $issueUri = URI->new(GITHUB_API_ENDPOINT);
			#$issueUri->path_segments("","repos",$owner,$repo,'issues');
			#my($issueSuccess,$issueData) = fetchJSON($issueUri,$ua,$gh_user,$gh_token,GITHUB_API_V_HEADER);
			#if($issueSuccess) {
			#	use Data::Dumper;
			#	print Dumper($issueData),"\n";
			#}
			
			# Versions
			$ans{'tool_versions'} = \@versions  if(scalar(@versions) > 0);
			# credits
			$ans{'tool_developers'} = \@contributors  if(scalar(@contributors) > 0);
		}
		
		return $githubRepoDataCache{$owner}{$repo};
	}	
}


my @opebTools = ();

my $save_opeb_filename = undef;
my $load_opeb_filename = undef;
my $config_filename = undef;

my $tabfile = undef;
my $jsondir = undef;
Getopt::Long::GetOptions(
	"save-opeb:s"	=>	\$save_opeb_filename,
	"use-opeb:s"	=>	\$load_opeb_filename,
	"config|C:s"	=>	\$config_filename,
	"file|f:s"	=>	\$tabfile,
	"directory|D:s"	=>	\$jsondir,
) or die("Error in command line arguments\n");

my $config;
my $gh_user = undef;
my $gh_token = undef;
if(defined($config_filename)) {
	$config = Config::IniFiles->new( -file => $config_filename );
	if(defined($config)) {
		$gh_user = $config->val('github','gh-user');
		$gh_token = $config->val('github','gh-token');
	} else {
		foreach my $croak (@Config::IniFiles::errors) {
			Carp::carp($croak);
		}
		exit 1;
	}
} else {
	# Empty config
	$config = Config::IniFiles->new();
}

if(defined($jsondir) || defined($tabfile)) {
	my $opEb = OpenEBenchQueries->new($load_opeb_filename,$save_opeb_filename);
	my $p_queries = $opEb->extractGitHubIds();

	if(scalar(@{$p_queries})>0) {
		my $jsonManifestFile = undef;
		my @manifest = ();
		if(defined($jsondir)) {
			File::Path::make_path($jsondir);
			$jsonManifestFile = File::Spec->catfile($jsondir,'manifest.json');
			print "* JSON output directory set to $jsondir . Manifest file is $jsonManifestFile\n";
		}
		
		my $TAB;
		my $jTAB;
		if(defined($tabfile)) {
			print "* Tabular output file set to $tabfile\n";
			open($TAB,'>:encoding(UTF-8)',$tabfile) or die("ERROR: Unable to create $tabfile: $!\n");
			$jTAB = JSON::MaybeXS->new();
		}
		
		unless(defined($jsondir) || defined($tabfile)) {
			print "* Default to tabular output file set to STDOUT\n";
			open($TAB,'>&:encoding(UTF-8)',\*STDOUT) or die("ERROR: Unable to redirect to STDOUT: $!\n");
		}
		
		print "* Processing ".scalar(@{$p_queries})." tools\n";
		
		my $printedHeader = undef;
		my $numTool = 0;
		my $ua = LWP::UserAgent->new();
		foreach my $query (@{$p_queries}) {
			# What we know, just now
			my %fullans = %{$query};
			
			foreach my $fullrepo (@{$fullans{'repos'}}) {
				my $p_ans = $fullrepo->{'res'} = getGitHubRepoData(%{$fullrepo},$gh_user,$gh_token,$ua);
				
				if(defined($TAB)) {
					unless($printedHeader) {
						$printedHeader = 1;
						print $TAB join("\t",map { $_->[1] } @TabKeyOrder),"\n";
						print $TAB join("\t",map { $_->[0] } @TabKeyOrder),"\n";
					}
					
					print $TAB join("\t",map {
						my $key = $_->[0];
						my $retval = '';
						if(exists($p_ans->{$key}) && defined($p_ans->{$key})) {
							$retval = $p_ans->{$key};
							if(ref($retval) eq 'ARRAY') {
								if(scalar(@{$retval}) > 0) {
									if(ref($retval->[0])) {
										$retval = join(',',map { $jTAB->encode($_) } @{$retval});
									} else {
										$retval = join(',',@{$retval});
									}
								}
							} elsif(ref($retval)) {
								$retval = $jTAB->encode($retval);
							}
						}
						$retval;
					} @TabKeyOrder),"\n";
				}
			}
			
			# The assembled answer
			if(defined($jsondir)) {
				my $partialJsonout = 'tool-'.$numTool.'.json';
				push(@manifest,{
					'@id'	=>	$fullans{'@id'},
					'file'	=>	$partialJsonout
				});
				my $jsonout = File::Spec->catfile($jsondir,$partialJsonout);
				if(open(my $J,'>:encoding(UTF-8)',$jsonout)) {
					print $J JSON::MaybeXS->new(pretty => 1)->encode(\%fullans);
					
					close($J);
				} else {
					Carp::croak("* ERROR: Unable to create file $jsonout. Reason: $!");
				}
			}
			# print JSON->new()->pretty(1)->encode(\%ans),"\n";
			
			# Another more
			$numTool++;
		}
		
		# Closing the output file (if any)
		close($TAB)  if(defined($TAB));
		
		# Writing the manifest
		if(defined($jsonManifestFile)) {
			if(open(my $M,'>:encoding(UTF-8)',$jsonManifestFile)) {
				print $M JSON::MaybeXS->new(pretty => 1)->encode(\@manifest);
				close($M);
			} else {
				Carp::croak("ERROR: Unable to write manifest $jsonManifestFile. Reason: $!");
			}
		}
	} else {
		print STDERR "No queries extracted from OpenEBench. Do you have internet access?\n";
	}
} else {
	print STDERR "Usage: $0 [-C config file] [-D destination directory | -f destination file] [--save-opeb save_opeb_file.json] [--use-opeb use_opeb_file.json]\n";
	exit 1;
}
