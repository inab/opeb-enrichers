# Installation instructions for githubEnricher



* You need to have `cpan` in order to install the dependencies declared at [cpanfile](cpanfile). `cpan` is available in many Linux distributions (Ubuntu package `perl`, CentOS package `perl-CPAN`), and also at [App::Cpan](http://search.cpan.org/~andk/CPAN-2.16/) Perl package.

* [Carton](https://metacpan.org/pod/Carton) is used to install the program dependencies in the `deps` directory, so your Perl installation is not polluted. As the program could not be available, these are the commands (which use `cpan`) to run:

```bash
perl -MCarton -c -e '' || cpan -i Carton
carton install -p deps --deployment
```

