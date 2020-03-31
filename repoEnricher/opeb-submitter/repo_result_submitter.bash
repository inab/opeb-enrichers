#!/bin/bash

#OPEB_HOST=openebench.bsc.es
#OPEB_METHOD=PATCH

OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"
OPEB_METHOD=POST
ALAMBIQUE_HOST="${ALAMBIQUE_HOST:-${OPEB_HOST}}"
ALAMBIQUE_METHOD=PUT

OPEB_METRICS_BASE=https://${OPEB_HOST}/monitor/metrics/
OPEB_ALAMBIQUE_BASE=https://${ALAMBIQUE_HOST}/monitor/alambique/

scriptdir="$(dirname "$0")"

case "$scriptdir" in
	/*)
		true
		;;
	*)
		scriptdir="${PWD}/${scriptdir}"
		;;
esac

if [ $# -gt 1 ]; then
	config="$1"
	shift
	# Loading the credentials
	source "$config"

	# These could have been redefined
	OPEB_HOST="${OPEB_HOST:-openebench.bsc.es}"
	OPEB_METHOD="${OPEB_METHOD:-POST}"
	ALAMBIQUE_HOST="${ALAMBIQUE_HOST:-${OPEB_HOST}}"
	ALAMBIQUE_METHOD="${ALAMBIQUE_METHOD:-PUT}"

	OPEB_METRICS_BASE="${OPEB_METRICS_BASE:-https://${OPEB_HOST}/monitor/metrics/}"
	OPEB_ALAMBIQUE_BASE="${OPEB_ALAMBIQUE_BASE:-https://${ALAMBIQUE_HOST}/monitor/alambique/}"

	if [ -n "$OPEB_METRICS_USER" -a -n "$OPEB_METRICS_PASS" ]; then
		jq --slurp --arg host "$OPEB_HOST" -f "$scriptdir"/repo_enricher2opeb.jq "$@" | \
		curl -v -X "$OPEB_METHOD" -u "${OPEB_METRICS_USER}":"${OPEB_METRICS_PASS}" -H 'Content-Type: application/json' \
		"${OPEB_METRICS_BASE}" -d "@-"
	else
		echo "ERROR: The config file must declare the OPEB_METRICS_USER and OPEB_METRICS_PASS variables, which are the credentials to OpenEBench" 1>&2
	fi
else
	echo "Usage: $0 {config_file} {json_result}+" 1>&2
fi
