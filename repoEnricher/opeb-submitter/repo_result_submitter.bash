#!/bin/bash

#OPEB_HOST=openebench.bsc.es
#OPEB_METHOD=PATCH

OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"
OPEB_METHOD=POST
ALAMBIQUE_HOST="${ALAMBIQUE_HOST:-${OPEB_HOST}}"
ALAMBIQUE_METHOD=PUT

OPEB_METRICS_BASE=https://${OPEB_HOST}/monitor/metrics/
OPEB_ALAMBIQUE_BASE=https://${ALAMBIQUE_HOST}/monitor/alambique/

batchSize=1000

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

if [ $# -gt 1 ]; then
	config="$1"
	shift
	# Loading the credentials
	source "$config"
	if [ -d  "$1" ] ; then
		resdir="$1"
		if [ -f "${resdir}/manifest.json" ] ; then
			# declare -a ifiles=( $(jq -r '.[] | .file ' "$resdir"/manifest.json ) )
			declare -a ifiles=( $(jq -r '.entries.with_repo[] | .file ' "$resdir"/manifest.json ) )
			# This is needed to resolve relative directories
			cd "$resdir"
		else
			eecho "Usage: $0 {config_file} {json_result_dir_with_manifest|json_files}+"
			exit 1
		fi
	else
		declare -a ifiles=( "$@" )
	fi

	# These could have been redefined
	OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"
	OPEB_METHOD="${OPEB_METHOD:-POST}"
	ALAMBIQUE_HOST="${ALAMBIQUE_HOST:-${OPEB_HOST}}"
	ALAMBIQUE_METHOD="${ALAMBIQUE_METHOD:-PUT}"

	OPEB_METRICS_BASE="${OPEB_METRICS_BASE:-https://${OPEB_HOST}/monitor/metrics/}"
	OPEB_ALAMBIQUE_BASE="${OPEB_ALAMBIQUE_BASE:-https://${ALAMBIQUE_HOST}/monitor/alambique/}"

	if [ -n "$OPEB_METRICS_USER" -a -n "$OPEB_METRICS_PASS" ]; then
		eecho "NUM FILES TO PROCESS: ${#ifiles[@]}"
		for pos in $(seq 0 "${batchSize}" "$((${#ifiles[@]} - 1))") ; do
			eecho $'\t'"BATCH $pos"
			eecho
			jq --slurp --arg host "$OPEB_HOST" -f "$scriptdir"/repo_enricher2opeb.jq "${ifiles[@]:${pos}:${batchSize}}" | \
			curl -v -X "$OPEB_METHOD" -u "${OPEB_METRICS_USER}":"${OPEB_METRICS_PASS}" -H 'Content-Type: application/json' \
			"${OPEB_METRICS_BASE}" -d "@-"
		done
	else
		eecho "ERROR: The config file must declare the OPEB_METRICS_USER and OPEB_METRICS_PASS variables, which are the credentials to OpenEBench"
	fi
else
	eecho "Usage: $0 {config_file} {json_result_dir_with_manifest|json_files}+"
fi
