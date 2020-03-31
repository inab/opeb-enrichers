# Installation instructions for OpenEBench repoEnricher

* You need to have `cpan` in order to install the dependencies declared at [cpanfile](cpanfile). `cpan` is available in many Linux distributions (Ubuntu package `perl`, CentOS package `perl-CPAN`), and also at [App::Cpan](http://search.cpan.org/~andk/CPAN-2.16/) Perl package.

* You also need to have installed development versions of OpenSSL, needed by one of the dependencies in the cpanfile. In Ubuntu, you need to install both `libssl-dev` and `zlib1g-dev`:

```bash
apt-get -y install libssl-dev zlib1g-dev
```

* [Carton](https://metacpan.org/pod/Carton) is used to install the program dependencies in the `deps` directory, so your Perl installation is not polluted. As the program could not be available, these are the commands (which use `cpan`) to run:

```bash
perl -MCarton -c -e '' || cpan -i Carton
carton install -p deps --deployment
```

* If you upgrade your installed Perl version, or you move this folder to a different location after following this instructions you may need to reinstall the dependencies. In that case, you should remove the `deps` directory and follow again the installation instructions.

# Metrics uploading to OpenEBench

This task is done by the scripts you can find at [opeb-submitter](opeb-submitter). Please follow install and usage instructions there.
