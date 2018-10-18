# Contributing

Thank you for considering contributing to CM-Well. We're still getting started at this so this policy can and will change from time to time.

Our [README](Readme.md) describes what CM-Well is and why it exists. You should read that first.

All contributions to CM-Well become licensed under our Apache 2 [license](https://github.com/CM-Well/CM-Well/blob/master/LICENSE)

We [discuss](https://gitter.im/CM-Well/CM-Well) CM-Well on Gitter and you're welcome to ask questions there.

## Reporting security issues

We take security seriously. If you believe you have found a security vulnerability please e-mail us directly: firstname.lastname@tr.com. *Do not* post a public security issue.


## Reporting other issues

Use issues to describe bugs and other issues you're having with CM-Well. Please consult the [database](https://github.com/CM-Well/CM-Well/issues) of open issues before creating a new one to ensure that you're not creating a duplicate.

When reporting an issue, please include steps to reproduce and ensure that you are clear about the version number of CM-Well that you're reporting the issue for.

## Contributing code and documentation

We use pull requests to suggest enhancements or changes to CM-Well. These don't have to be big! We'll happily take a pull request that fixes a typo in our docs.

To start, create your own fork of the project and make all your changes in there (see below). Push changes to your fork and submit the pull request via github.

Make sure you have a clear commit message that [describes](http://chris.beams.io/posts/git-commit/) your changes.

Our maintainers will review each request and suggest enhancements or improvements via the comments feature. We expect you to match the coding style of CM-Well and implement tests as appropriate to prove the validity of your enhancement.

By submitting a pull request, you certify that this is your own work that you wish to license under the CM-Well license. We do not accept anonymous pull requests.

## Forking CM-Well

The basic process for forking CM-Well is
* Login to github
* Press the fork button to the main CM-Well page to create your own fork of CM-Well in your space.
* Pull this down to your local machine
* Once you have a copy of your fork locally, update the git project to indicate the main remote site:
``git remote add cm-well-orig https://github.com/CM-Well/CM-Well.git``
* Disable push to the master repository: ``git remote set-url --push cm-well-orig DISABLE``
* Make your changes and use ``git push`` to push to your fork.
* To update your fork to the latest on master use ``git pull cm-well-orig master``
