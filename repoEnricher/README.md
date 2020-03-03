# OpenEBench repoEnricher

This Perl program fetches from OpenEBench technical toolbox REST API (which follows [next JSON Schema](https://openebench.bsc.es/monitor/tool/tool.json), [source from repo here](https://github.com/inab/elixibilitas/blob/master/java/elixibilitas-rest/src/main/resources/META-INF/resources/tool.json)) the repository URLs, and it generates a JSON with all the metadata an metrics extracted using the repository provider APIs. This tool currently acknowledges GitHub repositories, but it is being improved in order to add support to BitBucket and Gitlab.

File [INSTALL.md](INSTALL.md) contains the instructions to install this program.

In order to use this tool with no ban, you have to obtain next information:

* For GitHub, you need to [get a personal access token](https://github.com/settings/tokens) with `public_repo`, `read:user` and `user:email` privileges,  associated to a GitHub account.

* For BitBucket, you need to [get an application token](https://confluence.atlassian.com/bitbucket/app-passwords-828781300.html) with account, repositories, pull requests and issues read privileges, associated to a BitBucket account.

Then, you have to create a configuration file like [sample-config.ini](sample-config.ini) with the obtained personal tokens and additional information, so you can call the tool in the next way.

```bash
perl repoEnricher.pl --config myConfig.ini --directory=output
```

The supported parameters are:

* `--save-opeb`: The fetched entries from OpenEBench are saved in the file given with this parameter.

* `--use-opeb`: Instead of fetching entries from OpenEBench, use the file given with this parameter, which should follow OpenEBench JSON format.

* `--config` or `-C`: Configuration file where GitHub and BitBucket credentials are available, as well as other configuration details, like maximum number of queries per hour.

* `--file` or `-f`: The output of the program is going to be a tabular file with the name given with this parameter.

* `--directory` or `-D`: The output of the program is going to be a directory (with the name give with this parameter), where a JSON file is created for each one of the entries. Also, a manifest file is created in the directory once the program finishes properly.