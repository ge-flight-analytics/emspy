# Releasing 

Run the following steps to release a new version of `emspy`:

```
git checkout v0.2.1
git pull
pip install -r dev-requirements.txt
git config --global core.safecrlf false
bump2version patch
git push origin v0.2.1 --tags
python setup.py sdist bdist_wheel
twine upload dist/*
```

* Make sure the python dev dependencies are installed by running `pip install -r dev-requirements.txt`
* Stash or commit any pending changes to the repository
* On Windows, set `git config --global core.safecrlf false`. You probably want to turn this back on afterwards if it was set to true
* If this is a feature release, run `bump2version minor`. If this is a bug fix release, run `bump2version patch`
    * This will increment the version numbers in the repository, commit the changes, and create a new git tag
* Push the new commit and the tag to github with `git push origin v0.2.1 --tags`
* Generate packages using `python setup.py sdist bdist_wheel`
* Upload to pypi using `twine upload dist/*`

 Steps were generated using [this document](https://realpython.com/pypi-publish-python-package/)