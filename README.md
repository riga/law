<img src="https://raw.githubusercontent.com/riga/law/master/logo.png" alt="law logo" height="100"/>
-

[![Build Status](https://travis-ci.org/riga/law.svg?branch=master)](https://travis-ci.org/riga/law) [![Documentation Status](https://readthedocs.org/projects/law/badge/?version=latest)](http://law.readthedocs.io/en/latest/?badge=latest) [![Package Status](https://img.shields.io/pypi/v/law.svg)](https://pypi.python.org/pypi/law)

High-level extension layer for [Luigi](https://github.com/spotify/luigi) analysis workflows.


#### Installation and dependencies

Install via [pip](https://pypi.python.org/pypi/tfdeploy):

```bash
pip install law
```

This should also install [luigi](https://pypi.python.org/pypi/luigi) and [six](https://pypi.python.org/pypi/six).


### Content

- [TODO](#todo)
- [Development](#development)
- [Authors](#authors)
- [License](#license)


## TODO

##### Config

- `core`:
	- `db_file`: The path to the db file. Defaults to `$HOME/.law/db`
	- `target_tmp_dir`: The directory where tmp targets are stored. Defaults to ` tempfile.gettempdir()`.
- `paths`: Listing of paths to look for tasks when creating the db file.


##### Environment variables

- `LAW_CONFIG_FILE`
- `LAW_DB_FILE`
- `LAW_SANDBOX`
- `LAW_SANDBOX_SWITCHED`


##### Enable bash completion

```bash
source `law-completion`
```

## Development

- Source hosted at [GitHub](https://github.com/riga/law)
- Report issues, questions, feature requests on [GitHub Issues](https://github.com/riga/law/issues)

To start developing, simply load the `dev.sh`:

```bash
source dev.sh
```

In the future, there might be a docker image for development.


## Authors

- [Marcel R.](https://github.com/riga)


## License

The MIT License (MIT)

Copyright (c) 2016 Marcel R.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
