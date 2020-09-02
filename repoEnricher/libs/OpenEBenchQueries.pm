#!/usr/bin/perl

use strict;
use warnings 'all';

use Carp;
use HTTP::Request qw();
use JSON::MaybeXS;
use LWP::UserAgent qw();
use URI;

use RepoMatcher::Abstract;

package OpenEBenchQueries;

use Scalar::Util qw(blessed);

use Class::Load qw();

# Constant and variable declaration

use constant OPENEBENCH_SOURCE	=>	'https://openebench.bsc.es/monitor/rest/search';

my @MATCHERS = (
	'RepoMatcher::GitHub',
	'RepoMatcher::BitBucket'
);

# Method bodies

sub new(;$$$$$) {
	my $self = shift;
	my $class = ref($self) || $self;
	
	$self = {}  unless(ref($self));
	
	my($load_opeb_filename,$save_opeb_filename,$config,$ua,$p_matchers) = @_;
	
	Carp::croak('Expected a Config::IniFiles instance as first parameter')  unless(blessed($config) && $config->isa('Config::IniFiles'));
	
	$p_matchers = \@MATCHERS  unless(ref($p_matchers) eq 'ARRAY');
	
	# Variable initialization
	$self->{'load_opeb_filename'} = $load_opeb_filename;
	$self->{'save_opeb_filename'} = $save_opeb_filename;
	$self->{'ua'} = (blessed($ua) && $ua->isa('LWP::UserAgent')) ? $ua : LWP::UserAgent->new();
	
	# Instances initialization
	my @rmInstances = ();
	foreach my $rmclazz (@{$p_matchers}) {
		my($success,$errmsg) = Class::Load::try_load_class($rmclazz);
		if($success) {
			push(@rmInstances,$rmclazz->new($config,$self->{'ua'}))
		} else {
			Carp::croak("Unable to load class $rmclazz . Reason: $errmsg");
		}
	}
	
	$self->{'repo_matchers'} = \@rmInstances;
	
	return bless($self,$class);
}

sub extractQueryableRepoIds(;$) {
	my $self = shift;
	
	my($sourceURL) = @_;
	
	$sourceURL = OPENEBENCH_SOURCE  unless(defined($sourceURL));
	
	my $ua = $self->{'ua'};
	
	my $raw_opeb;
	if(defined($self->{'load_opeb_filename'})) {
		if($self->{'load_opeb_filename'} =~ /\.xz$/ && open(my $XOP,'-|:encoding(UTF-8)','unxz','-c',$self->{'load_opeb_filename'})) {
			undef $/;
			$raw_opeb = <$XOP>;
			close($XOP);
		} elsif(open(my $OP,'<:encoding(UTF-8)',$self->{'load_opeb_filename'})) {
			undef $/;
			$raw_opeb = <$OP>;
			close($OP);
		} else {
			Carp::croak("Error reading input OpenEBench payload ".$self->{'load_opeb_filename'}." . Reason: $!");
		}
	} else {
		$ua = LWP::UserAgent->new()  unless(blessed($ua) && $ua->isa('LWP::UserAgent'));
		my $req = HTTP::Request->new('GET' => OPENEBENCH_SOURCE);
		
		my $response = $ua->request($req);
		
		my $retval = undef;
		if($response->is_success()) {
			$raw_opeb = $response->decoded_content();
		} else {
			Carp::croak("ERROR: kicked out: ".$response->status_line());
		}
	}
	
	# Saving the payload
	if(defined($self->{'save_opeb_filename'})) {
		if(open(my $OP,'>:encoding(UTF-8)',$self->{'save_opeb_filename'})) {
			print $OP $raw_opeb;
			
			close($OP);
		} else {
			Carp::croak("Error saving input OpenEBench payload ".$self->{'save_opeb_filename'}." . Reason: $!");
		}
	}
	
	# Now, parse it 
	my $retval = JSON::MaybeXS->new()->decode($raw_opeb);

	if($@) {
		Carp::croak("ERROR: JSON parsing error: $@");
	}
	
	# Allowing single entries
	$retval = [ $retval ]  unless(ref($retval) eq 'ARRAY');
	
	return $self->parseOpenEBench($retval);
}

my %Features = (
	'documentation'	=>	{
		'general'	=>	undef,
		'manual'	=>	undef
	},
	'distributions' => {
		'source_packages'	=>	undef,
		'binary_packages'	=>	undef,
		'sourcecode'	=>	undef,
		'binaries'	=>	undef
	},
	'web'	=>	{
		'homepage'	=>	undef
	},
	'homepage'	=>	undef,
	'repositories'	=>	undef
);

sub _linkExtract(\%\%);

sub _linkExtract(\%\%) {
	my($p_entry,$p_features) = @_;
	
	my @entry_links = ();
	
	foreach my $mkey (keys(%{$p_features})) {
		if(exists($p_entry->{$mkey}) && defined($p_entry->{$mkey})) {
			my $s_features = $p_features->{$mkey};
			
			if(defined($s_features)) {
				my $s_entry = $p_entry->{$mkey};
				push(@entry_links,_linkExtract(%{$s_entry},%{$s_features}));
			} elsif(ref($p_entry->{$mkey}) eq 'ARRAY') {
				foreach my $it (@{$p_entry->{$mkey}}) {
					push(@entry_links,$it)  if(defined($it) && length($it) > 0);
				}
			} elsif(length($p_entry->{$mkey}) > 0) {
				push(@entry_links,$p_entry->{$mkey});
			}
		}
	}
	
	return @entry_links;
}

sub parseOpenEBench(\@) {
	my $self = shift;
	
	my($p_entries) = @_;
	
	my @queries = ();
	
	my $ua = $self->{'ua'};
	foreach my $entry (@{$p_entries}) {
		my @entry_links = _linkExtract(%{$entry},%Features);
		
		if(scalar(@entry_links) > 0) {
			my %repoEntries = ();
			my @repos = ();
			
			foreach my $entry_link (@entry_links) {
				foreach my $rm (@{$self->{'repo_matchers'}}) {
					my($isURI,$workspace,$repo) = $rm->doesMatch($entry_link);
					
					if($isURI && defined($workspace) && (length($workspace) > 0) && defined($repo) && (length($repo) > 0)) {
						# Due GitHub behaves, it is case insensitive
						my $lcWorkspace = lc($workspace);
						my $lcRepo = lc($repo);
						my $kind = $rm->kind();
						
						$repoEntries{$kind} = {}  unless(exists($repoEntries{$kind}));
						$repoEntries{$kind}{$lcWorkspace} = {}  unless(exists($repoEntries{$kind}{$lcWorkspace}));
						unless(exists($repoEntries{$kind}{$lcWorkspace}{$lcRepo})) {
							my $p_links = [];
							$repoEntries{$kind}{$lcWorkspace}{$lcRepo} = $p_links;
							push(@repos,{
								'kind'	=>	$kind,
								'instance'	=>	$rm,
								'owner'	=>	$workspace,
								'workspace'	=>	$workspace,
								'repo'	=>	$repo,
								'links'	=>	$p_links
							});
						}
						
						# Gathering
						push(@{$repoEntries{$kind}{$lcWorkspace}{$lcRepo}},$entry_link);
						last;
					}
				}
			}
			
			# Return only those ones with something interesting
			if(scalar(@repos) > 0) {
				push(@queries,{
					'@id' =>	$entry->{'@id'},
					'entry_links'	=>	\@entry_links,
					'repos'	=>	\@repos
				});
			}
		}
	}
	
	return \@queries;
}

1;
