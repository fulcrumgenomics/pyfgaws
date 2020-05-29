# pyfgaws

# Getting Setup

Conda is used to install a specific version of python and [poetry](https://github.com/python
-poetry/poetry) which is then used to manage the python development environment.  If not already
 installed, install [miniconda from the latest platform-appropriate installer](miniconda-link
 ). Then run:

```
conda create -n pyfgaws -c bioconda --file conda-requirements.txt
```

Then activate the new environment and install the toolkit:

```
conda activate pyfgaws
poetry install
```

[miniconda-link]: https://docs.conda.io/en/latest/miniconda.html