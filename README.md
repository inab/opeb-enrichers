[![DOI](https://zenodo.org/badge/120434235.svg)](https://zenodo.org/badge/latestdoi/120434235)

# OpenEBench Enrichers

Currently there are two enrichers, which take the OpenEBench entries following [next JSON Schema](https://openebench.bsc.es/monitor/tool/tool.json) ([source from repo here](https://github.com/inab/elixibilitas/blob/master/java/elixibilitas-rest/src/main/resources/META-INF/resources/tool.json)), and gather additional information based on each one of them.

* [Publications enricher](pubEnricher): It takes the recorded publication ids (pmid, DOI and/or PMID), validate them, and it gathers additional information, like the journal, year, authors, references and citations.

* [Repository enricher](repoEnricher): It takes the recorded links pointing to source code repository resources, and it fetches additional information provided by APIs like [GitHub API](https://developer.github.com/v3/): tags, releases, commits, branches, authors, licences, etc...

## OpenEBench Enrichers requirements

See [pubEnricher](pubEnricher/INSTALL.md) and [repoEnricher](repoEnricher/INSTALL.md) requirements.

