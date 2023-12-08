# Release Guidelines for PangaeaPy

This document specifies release guidelines for the PANGAEA Python Client.

## General Prerequisites

* All tests are passing.
* CHANGELOG.md is up-to-date.
* dependencies in `setup.cfg` are up-to-date.

## Steps

1. Update CHANGELOG.md: Replace the "Unreleased" heading by `<VERSION> - <date>`.

2. Check all general prerequisites.

3. Update the version:
   - `version` variables in `src/doc/conf.py`
   - Version in [setup.cfg](./setup.cfg)

4. Tag the latest commit of the master branch with `<VERSION>`.

5. Remove possibly existing `./dist` directory with old release.

6. Publish the release by executing `./release.sh` with uploads the pangeapy
   module to the Python Package Index [pypi.org](https://pypi.org).

7. After the release, start a new development version by increasing at least the
   micro version in [setup.cfg](./setup.cfg) and preparing CHANGELOG.md by
   adding a new "Unreleased" section on top.
