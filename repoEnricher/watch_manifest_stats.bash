#!/bin/bash

exec watch -x jq 'def roundit: .*1000.0|round/1000.0; { "@timestamp": ."@timestamp", "with": .entries.with_repo | length , "no": .entries.no_repo | length , "with_p": ((.entries.with_repo | length) / ((.entries.with_repo | length) + (.entries.no_repo | length))) | roundit, "no_p": ((.entries.no_repo | length) / ((.entries.with_repo | length) + (.entries.no_repo | length))) | roundit, "total": ((.entries.with_repo | length) + (.entries.no_repo | length)) }' "$1"
