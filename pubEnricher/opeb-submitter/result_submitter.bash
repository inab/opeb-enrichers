#!/bin/bash

#OPEB_HOST=openebench.bsc.es
#OPEB_METHOD=PATCH

OPEB_HOST=dev-openebench.bsc.es
OPEB_METHOD=POST

OPEB_METRICS_BASE=https://${OPEB_HOST}/monitor/metrics/

set -e

scriptdir="$(dirname "$0")"

case "$scriptdir" in
	/*)
		true
		;;
	*)
		scriptdir="${PWD}/${scriptdir}"
		;;
esac

eecho () {
	echo "$@" 1>&2
}

doProcessing () {
	# Classifying the input
	local -a files
	local -a dirs
	local -a discarded
	for file in "$@" ; do
		if  [ -r "$file" ] ; then
			if [ -f "$file" ] ; then
				files+=( "$file" )
			elif [ -d "$file" ] ; then
				dirs+=( "$file" )
			else
				discarded+=( "$file" )
			fi
		else
			discarded+=( "$file" )
		fi
	done
	
	if [ ${#discarded[@]} -gt 0 ] ; then
		eecho "DISCARDED / UNREADABLE: ${#discarded[@]}"
		eecho $'\t'"${discarded[@]}"
		eecho
	fi
	
	if [ ${#files[@]} -gt 0 ] ; then
		eecho "NUM FILES TO PROCESS: ${#files[@]}"
		eecho
		jq --slurp --arg host "$OPEB_HOST" -f "$scriptdir"/enricher2opeb.jq "${files[@]}" | \
		curl -v -X "${OPEB_METHOD}" -u "${OPEB_METRICS_USER}":"${OPEB_METRICS_PASS}" -H 'Content-Type: application/json' \
		"${OPEB_METRICS_BASE}" -d "@-"
	fi
	
	# Last, the directories
	if [ ${#dirs[@]} -gt 0 ] ; then
		eecho "DIRS TO PROCESS: ${#dirs[@]}"
		eecho
		for dir in "${dirs[@]}" ; do
			eecho "PROCESSING DIR $dir"
			eecho
			
			doProcessing "$dir"/*
		done
	fi
}

if [ $# -gt 1 ]; then
	config="$1"
	shift
	
	# Loading the credentials
	source "$config"
	
	if [ -n "$OPEB_METRICS_USER" -a -n "$OPEB_METRICS_PASS" ]; then
		doProcessing "$@"
	else
		eecho "ERROR: The config file must declare the OPEB_METRICS_USER and OPEB_METRICS_PASS variables, which are the credentials to OpenEBench"
	fi
else
	eecho "Usage: $0 {config_file} {json_result or dir}+"
fi
