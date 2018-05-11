#!/bin/bash

OPEB_METRICS_BASE=https://openebench.bsc.es/monitor/metrics
OPEB_METRICS_USER=pubenricher
OPEB_METRICS_PASS=ObKavVar7OvImwufineds

scriptdir="$(basedir "$0")"

case "$scriptdir" in
	/*)
		true
		;;
	*)
		scriptdir="${PWD}/${scriptdir}"
		;;
esac

#JQTRANS=<<EOF
#{
#	"@id": .["@id"],
#	"project": {
#		"publications": ( .entry_pubs | { "doi": .doi } )
#	}
#}
#EOF
#
#jq "$JQTRANS" entry_12873.json


if [ $# > 0 ]; then
#	for A in "$@" ; do
		# Get the id
#		tool_id="$(jq -r '.["@id"]' "$A" | cut -d '/' -f 6- )"
#		
#		jq "$JQTRANS" "$A"
		
#		echo "Storing tool ${tool_id}"
		jq --slurp -f "$scriptdir"/enricher2opeb.jq "$@" | \
		curl -v -X PATCH -u "${OPEB_METRICS_USER}":"${OPEB_METRICS_PASS}" -H 'Content-Type: application/json' \
		"${OPEB_METRICS_BASE}" -d "@-"
#	done
fi
