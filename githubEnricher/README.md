# biotools2github

This Perl program fetches from OpenEBench the GitHub URLs, and it generates a JSON with all the metadata extracted using the GitHub APIs.

You can install this program dependencies in a local virtual environment with the next recipe

```bash
# Install App::Virtualenv if it is not installed yet
perl -MApp::Virtualenv -c -e '' || cpan -i App::Virtualenv
# If past step installs local::lib, modifying your .bashrc, then reload
# your profile before following with the next steps
virtualenv.pl .vperlenv
source .vperlenv/bin/activate
perl -MApp::cpanminus -c -e '' ||cpan -i App::cpanminus
cpanm --installdeps .
```

In order to use this tool with no ban from the GitHub server side, you need to get a developer API key, and pass the GitHub user and the API key to the script.

```bash
perl "biotools2github.pl "--gh-user=YOUR_USER" "--gh-token=YOUR_API_TOKEN" --jsondir=output
```