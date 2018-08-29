# Installation instructions for githubEnricher

You need to have either `cpanm` or `cpan` in order to install the dependencies declared at [cpanfile](cpanfile).

If you don't have `cpanm`, the sentence to install it is:

```bash
perl -MApp::cpanminus -c -e '' || cpan -i App::cpanminus
```

Optionally, you can create a separate virtual environment, in order to avoid polluting you profile:


```bash
# Install App::Virtualenv if it is not installed yet
perl -MApp::Virtualenv -c -e '' || cpanm App::Virtualenv
# If past step installs local::lib, modifying your .bashrc, then reload
# your profile before following with the next steps
virtualenv.pl .plenv
source .plenv/bin/activate
```

The required modules are installed running this:

```bash
cpanm --installdeps .
```
