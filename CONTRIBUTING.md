# Contributing

Some help/guidelines if you want to contribute to this library.

## General Overview


## Style

Use `flake8` to check PEP8 and pylint errors. In the root directory, just run `flake8` - it will automatically use the settings in [tox.ini](tox.ini). Note that I'm not too fastidious about line length - so long as it's below 100 chars, it should be fine - I'd rather something sensible and slightly longer than sprawling over many lines.

Indents are 4 spaces. `CamelCase` for classes, `lower_case` for functions and variable names. The exception is a module-wide variable, which should be `ALL_CAPS`.

Docstrings should use be in numpy style.