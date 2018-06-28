# OpenEBench references and citations enricher

This program is written for Python 3.5 and later. The installation procedure is in [INSTALL.md](INSTALL.md).

## Usage
Once the program is installed, and its environment activated, you can see the different options using `-h` flag:

```
usage: pubEnricher.py [-h] [-F] [--fully-annotated] [-d]
                      [-b {europepmc,pubmed,wikidata,meta}]
                      [-C CONFIG_FILENAME] [--save-opeb SAVE_OPEB_FILENAME]
                      [--use-opeb LOAD_OPEB_FILENAME]
                      (-D RESULTS_DIR | -f RESULTS_FILE)
                      [cacheDir]

positional arguments:
  cacheDir              The optional cache directory, to be reused

optional arguments:
  -h, --help            show this help message and exit
  -F, --full            Return the full gathered citation results, not the
                        citation stats by year
  --fully-annotated     Return the reference and citation results fully
                        annotated, not only the year
  -d, --debug           Show the URL statements
  -b {europepmc,pubmed,wikidata,meta}, --backend {europepmc,pubmed,wikidata,meta}
                        Choose the enrichment backend
  -C CONFIG_FILENAME, --config CONFIG_FILENAME
                        Config file to pass setup parameters to the different
                        enrichers
  --save-opeb SAVE_OPEB_FILENAME
                        Save the OpenEBench content to a file
  --use-opeb LOAD_OPEB_FILENAME
                        Use the OpenEBench content from a file instead of
                        network
  -D RESULTS_DIR, --directory RESULTS_DIR
                        Store each separated result in the given directory
  -f RESULTS_FILE, --file RESULTS_FILE
                        The results file, in JSON format
```

Although a config file is not needed to run the program, it is needed to customize its behavior. A sample config file is available at [sample-config.ini](sample-config.ini).
