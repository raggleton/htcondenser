# Contributing

Some help/guidelines if you want to contribute to this library.

## General Overview

1) Get a Github account

2) Fork this repo to your own repo

3) Clone & install it, either by using `setup.sh`, or ``pip install -e <path to clone>``

4) Hack away, commit (preferably to a new topic branch)

5) Push back to your repo

6) Make a Pull Request

## Style

Use `flake8` to check PEP8 and pylint errors. In the root directory, just run `flake8` - it will automatically use the settings in [tox.ini](tox.ini). Note that I'm not too fastidious about line length - so long as it's below 100 chars, it should be fine. I'd rather something sensible and slightly longer than sprawling over many lines.

Indents are 4 spaces. `CamelCase` for classes, `lower_case` for functions and variable names. The exception is a module-wide variable, which should be `ALL_CAPS`.

Docstrings should be in numpy style, suitable for use with sphinx + napoleon extension: [https://sphinxcontrib-napoleon.readthedocs.org/en/latest/](https://sphinxcontrib-napoleon.readthedocs.org/en/latest/)

To make the documentation, do:

```
cd docs
make html  # or latexpdf or ...
```