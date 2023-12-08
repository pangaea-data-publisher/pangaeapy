# This is a hack for:
# https://github.com/pypa/pip/issues/7953

import setuptools
import site
import sys
site.ENABLE_USER_SITE = "--user" in sys.argv[1:]

if __name__ == "__main__":
    setuptools.setup()
