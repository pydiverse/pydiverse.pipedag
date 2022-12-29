# Sharing code in tests folder is a bit tricky. Normally nothing is put on PYTHONPATH, there.
# There are a few techniques that still work:
#   1) import relative to test file with relative import
#   2) import relative to test file with without relative import and with tests.__init__.py
#   3) sys.path.append in conftest.py
#   4) putting tests folder on PYTHONPATH in project.toml / poetry run pytest (tricky to avoid copy to final package)
#   5) test-utility-package in same repo
#   6) private package directory in "src"
#
# We currently go with 1) as long as we hear complaints that it is not working for someone
