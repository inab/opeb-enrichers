# githubEnricher

This Perl program fetches from OpenEBench the GitHub URLs, and it generates a JSON with all the metadata extracted using the GitHub APIs.

File [INSTALL.md](INSTALL.md) contains the instructions to install this program.

In order to use this tool with no ban from the GitHub server side, you need to [get a personal access token](https://github.com/settings/tokens) with `public_repo`, `read:user` and `user:email` privileges,  associated to a GitHub account.

Then, you have to create a configuration file like [sample-config.ini](sample-config.ini) with the GitHub user and the obtained personal token, so you can call the tool in the next way.

```bash
perl githubEnricher.pl --config myConfig.ini --jsondir=output
```