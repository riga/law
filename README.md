<img src="https://raw.githubusercontent.com/riga/law/master/logo.png" alt="law logo" height="100"/>
-

[![Build Status](https://travis-ci.org/riga/law.svg?branch=master)](https://travis-ci.org/riga/law) [![Documentation Status](https://readthedocs.org/projects/law/badge/?version=latest)](http://law.readthedocs.io/en/latest/?badge=latest) [![Package Status](https://img.shields.io/pypi/v/law.svg)](https://pypi.python.org/pypi/law)

High-level extension layer for [Luigi](https://github.com/spotify/luigi) analysis workflows.


Environment variables:

- `LAW_CONFIG_FILE`
- `LAW_DB_FILE`


Config:

- `core`:
	- `db_file`: The path to the db file. Defaults to `$HOME/.law/db`
	- `target_tmp_dir`: The directory where tmp targets are stored. Defaults to ` tempfile.gettempdir()`.
- `paths`: Listing of paths to look for tasks when creating the db file.


Enable bash completion:

```
source `law-completion`
```
