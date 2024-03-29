# metaflow-data-etl

<img alt="Python" src="https://img.shields.io/badge/python-3.12-blue.svg" /> </a>
<img alt="GitHub" src="https://img.shields.io/github/license/huggingface/transformers.svg?color=blue"> </a>
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![Black](https://img.shields.io/badge/code%20style-black-000000.svg)


[black]: http://github.com/psf/black
[black-shield]: https://img.shields.io/badge/code%20style-black-black.svg?style=for-the-badge&labelColor=gray

![Visual Studio Code](https://img.shields.io/badge/Visual%20Studio%20Code-0078d7.svg?style=for-the-badge&logo=visual-studio-code&logoColor=white)  ![Python](https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54) ![Debian](https://img.shields.io/badge/Debian-D70A53?style=for-the-badge&logo=debian&logoColor=white) ![Docker](https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white) ![Poetry](https://img.shields.io/badge/Poetry-%233B82F6.svg?style=for-the-badge&logo=poetry&logoColor=0B3D8D) ![Pandas](https://img.shields.io/badge/pandas-%23150458.svg?style=for-the-badge&logo=pandas&logoColor=white)


A usecase designed to simulate some summary statistics from machine data.


#### Features

* Metaflow engine for running DAGs
* MyPy, Black, and Ruff for code cleanliness 
* Docker-compose for simulating CI

## Quickguide

To speed test that everything works:

```bash
make test.run # run all of the tests
make production.run  # exec metaflow in a container
```

* Make sure that data/ is populated with the `sample.parquet` file

## File Structures

1. `Makefile` contains helpful commands for how to startup different items
    * Contains ability to run things locally, as well as helper functions for running in "production" mode
2. Poetry is used as an environment manager
3. Metaflow is used as a way to execute tasks
4. Main code is split between core and helpers, with helpers split out for ease of testing
