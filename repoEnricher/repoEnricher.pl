#!/usr/bin/perl

use v5.10.1;
use strict;
use warnings 'all';

use File::Spec;
use FindBin;

# We cannot use local::lib because at this point we cannot be sure
# about having it installed
use lib File::Spec->catdir($FindBin::Bin,'deps','lib','perl5');

# Project additional libraries are available here
use lib File::Spec->catfile($FindBin::Bin,'libs');

use OpenEBenchQueries;
use RepoMatcher::Abstract;

use Carp;
use Config::IniFiles;

use Time::HiRes;

use JSON::MaybeXS;
use LWP::UserAgent qw();
use Scalar::Util qw(blessed);
use Getopt::Long;

use File::Path qw();

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
	my $ua = LWP::UserAgent->new();
	my $opEb = OpenEBenchQueries->new($load_opeb_filename,$save_opeb_filename,$config,$ua);
	my $p_queries = $opEb->extractQueryableRepoIds();
	
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
		foreach my $query (@{$p_queries}) {
			# What we know, just now
			my %fullans = %{$query};
			
			my $tool_id = exists($fullans{'@id'}) ? $fullans{'@id'} : '-';
			
			foreach my $fullrepo (@{$fullans{'repos'}}) {
				my $rm = $fullrepo->{'instance'};
				my $p_ans = $fullrepo->{'res'} = $rm->getRepoData(\%{$fullrepo});
				
				# Removing this
				delete($p_ans->{'instance'});
				
				if(defined($TAB)) {
					unless($printedHeader) {
						$printedHeader = 1;
						print $TAB join("\t",map { $_->[1] } @TabKeyOrder),"\n";
						print $TAB join("\t",map { $_->[0] } @TabKeyOrder),"\n";
					}
					# Synthetic
					unless(exists($p_ans->{'tool_id'})) {
						$p_ans->{'tool_id'} = $tool_id;
					}
					
					print $TAB join("\t",map {
						my $key = $_->[0];
						my $retval = '';
						if(exists($p_ans->{$key}) && defined($p_ans->{$key})) {
							$retval = $p_ans->{$key};
							my $reftype = ref($retval);
							if($reftype eq 'ARRAY') {
								if(scalar(@{$retval}) > 0) {
									if(ref($retval->[0])) {
										$retval = join(',',map { $jTAB->encode($_) } @{$retval});
									} else {
										$retval = join(',',@{$retval});
									}
								}
							} elsif($reftype eq 'HASH') {
								$retval = $jTAB->encode($retval);
							} elsif($reftype ne '') {
								# TODO: better checks
								# use Data::Dumper;
								# print STDERR "BUGREF\n",Dumper($retval),"\n";
								# exit(1);
								$retval = $$retval;
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
					print $J JSON::MaybeXS->new(pretty => 1,convert_blessed => 1, canonical => 1)->encode(\%fullans);
					
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
				print $M JSON::MaybeXS->new(pretty => 1, canonical => 1)->encode(\@manifest);
				close($M);
			} else {
				Carp::croak("ERROR: Unable to write manifest $jsonManifestFile. Reason: $!");
			}
		}
	} else {
		print STDERR "No queries extracted from OpenEBench. Do you have internet access?\n";
	}
} else {
	print STDERR "Usage: $0 [-C config file] [-D destination directory (json files) | -f destination file (tabular file)] [--save-opeb save_opeb_file.json] [--use-opeb use_opeb_file.json]\n";
	exit 1;
}
