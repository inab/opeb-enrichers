#!/bin/bash

#OPEB_HOST=openebench.bsc.es
#OPEB_METHOD=PATCH

OPEB_HOST=dev-openebench.bsc.es
OPEB_METHOD=POST

OPEB_METRICS_BASE=https://${OPEB_HOST}/monitor/metrics/

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
		jq --slurp --arg host "$OPEB_HOST" -f "$scriptdir"/enricher2opeb.jq "$@" | \
		curl -v -X "${OPEB_METHOD}" -u "${OPEB_METRICS_USER}":"${OPEB_METRICS_PASS}" -H 'Content-Type: application/json' \
		"${OPEB_METRICS_BASE}" -d "@-"
	else
		echo "ERROR: The config file must declare the OPEB_METRICS_USER and OPEB_METRICS_PASS variables, which are the credentials to OpenEBench" 1>&2
	fi
else
	echo "Usage: $0 {config_file} {json_result}+" 1>&2
fi
