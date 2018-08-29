[![DOI](https://zenodo.org/badge/120434235.svg)](https://zenodo.org/badge/latestdoi/120434235)

# OpenEBench Enrichers

Currently there are two enrichers, which take the OpenEBench entries, and gather additional information based on each one of them.

* [Publications enricher](pubEnricher): It takes the recorded publication ids (pmid, DOI and/or PMID), validate them, and it gathers additional information, like the journal, year, authors, references and citations.

* [GitHub enricher](githubEnricher): It takes the recorded links pointing to GitHub resources, and it fetches additional information provided by [GitHub API](https://developer.github.com/v3/): tags, releases, commits, branches, authors, licences, etc...

## OpenEBench Enrichers requirements

See [pubEnricher](pubEnricher/INSTALL.md) and [githubEnricher](githubEnricher/INSTALL.md) requirements.

