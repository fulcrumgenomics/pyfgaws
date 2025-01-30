
[![Language][language-badge]][language-link]
[![Code Style][code-style-badge]][code-style-link]
[![Type Checked][type-checking-badge]][type-checking-link]
[![PEP8][pep-8-badge]][pep-8-link]
[![Code Coverage][code-coverage-badge]][code-coverage-link]
[![License][license-badge]][license-link]

---

[![Python package][python-package-badge]][python-package-link]
[![PyPI version][pypi-badge]][pypi-link]
[![PyPI download total][pypi-downloads-badge]][pypi-downloads-link]

---

[language-badge]:       http://img.shields.io/badge/language-python-brightgreen.svg
[language-link]:        http://www.python.org/
[code-style-badge]:     https://img.shields.io/badge/code%20style-black-000000.svg
[code-style-link]:      https://black.readthedocs.io/en/stable/ 
[type-checking-badge]:  http://www.mypy-lang.org/static/mypy_badge.svg
[type-checking-link]:   http://mypy-lang.org/
[pep-8-badge]:          https://img.shields.io/badge/code%20style-pep8-brightgreen.svg
[pep-8-link]:           https://www.python.org/dev/peps/pep-0008/
[code-coverage-badge]:  https://codecov.io/gh/fulcrumgenomics/pyfgaws/branch/main/graph/badge.svg
[code-coverage-link]:   https://codecov.io/gh/fulcrumgenomics/pyfgaws
[license-badge]:        http://img.shields.io/badge/license-MIT-blue.svg
[license-link]:         https://github.com/fulcrumgenomics/pyfgaws/blob/main/LICENSE
[python-package-badge]: https://github.com/fulcrumgenomics/pyfgaws/workflows/Python%20package/badge.svg
[python-package-link]:  https://github.com/fulcrumgenomics/pyfgaws/actions?query=workflow%3A%22Python+package%22
[pypi-badge]:           https://badge.fury.io/py/pyfgaws.svg
[pypi-link]:            https://pypi.python.org/pypi/pyfgaws
[pypi-downloads-badge]: https://img.shields.io/pypi/dm/pyfgaws
[pypi-downloads-link]:  https://pypi.python.org/pypi/pyfgaws

# pyfgaws

Python tools and APIs for working with AWS.

<p>
<a href float="left"="https://fulcrumgenomics.com"><img src=".github/logos/fulcrumgenomics.svg" alt="Fulcrum Genomics" height="100"/></a>
</p>

[Visit us at Fulcrum Genomics](www.fulcrumgenomics.com) to learn more about how we can power your Bioinformatics with pyfgaws and beyond.

<a href="mailto:contact@fulcrumgenomics.com?subject=[GitHub inquiry]"><img src="https://img.shields.io/badge/Email_us-brightgreen.svg?&style=for-the-badge&logo=gmail&logoColor=white"/></a>
<a href="https://www.fulcrumgenomics.com"><img src="https://img.shields.io/badge/Visit_Us-blue.svg?&style=for-the-badge&logo=wordpress&logoColor=white"/></a>

`pip install pyfgaws`

**Requires python 3.9**

# Getting Setup

Conda is used to install a specific version of python and [poetry][poetry-link]
which is then used to manage the python development environment.  If not already installed, install 
[miniconda from the latest platform-appropriate installer](miniconda-link). Then run:

```
conda create -n pyfgaws -c conda-forge -c bioconda --file conda-requirements.txt
```

Then activate the new environment and install the toolkit:

```
conda activate pyfgaws
poetry install
```

[miniconda-link]: https://docs.conda.io/en/latest/miniconda.html
[poetry-link]:    https://github.com/python-poetry/poetry
