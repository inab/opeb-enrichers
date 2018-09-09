#!/bin/bash

OPEB_METRICS_BASE=https://openebench.bsc.es/monitor/metrics/
OPEB_METHOD=PATCH

#OPEB_METRICS_BASE=https://dev-openebench.bsc.es/monitor/metrics/
#OPEB_METHOD=POST

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
	if [ -n "$OPEB_METRICS_USER" -a -n "$OPEB_METRICS_PASS" ]; then
		jq --slurp -f "$scriptdir"/gh_enricher2opeb.jq "$@" | \
		curl -v -X "$OPEB_METHOD" -u "${OPEB_METRICS_USER}":"${OPEB_METRICS_PASS}" -H 'Content-Type: application/json' \
		"${OPEB_METRICS_BASE}" -d "@-"
	else
		echo "ERROR: The config file must declare the OPEB_METRICS_USER and OPEB_METRICS_PASS variables, which are the credentials to OpenEBench" 1>&2
	fi
else
	echo "Usage: $0 {config_file} {json_result}+" 1>&2
fi
