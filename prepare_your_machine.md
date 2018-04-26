# Setup your machine for the workshop

## What you should have

Your machine should have the following installed :
* Python (>2.7.10 at least for Python 2). If you are on OSX, installing homebrew and the homebrew python is highly recommended as well.

* SQLite (It should be installed on most systems)

* Upgrading pip is recommended.
    
    ```bash
    pip install --upgrade pip
    ```


## Setup

### Get Virtualenv

I would recommend virtualenv for testing.

```bash
pip install --upgrade virtualenv
```

### Virtualenv

```bash
rm -rf airflow_workshop
virtualenv airflow_workshop
source airflow_workshop/bin/activate
```

### Installing Airflow

The easiest way to install the latest stable version of Airflow is with ``pip``:

```bash
pip install apache-airflow
```

The current stable version is ``1.9.0``. You can install this version specifically by using

```bash
pip install apache-airflow==1.9.0
```

You also need to install Airflow with support for ``gcp`` for the sake of this tutorial. Currently this only works for Python2 (check [Troubleshooting](#troubleshooting) ):

```bash
# Install the latest with extras
pip install apache-airflow[gcp_api]

# Install the 1.9.0 with specific extras
pip install apache-airflow[gcp_api]==1.9.0
```

The current list of `extras` is available [here](https://github.com/apache/incubator-airflow/blob/master/setup.py) and an older version can be found in the [docs](https://airflow.incubator.apache.org/installation.html#extra-packages).

### Troubleshooting
There seems to be some problem with Python3 & "gcp_api" integration.

There are issues open already on Airflow JIRA:
* https://issues.apache.org/jira/browse/AIRFLOW-2343
* https://issues.apache.org/jira/browse/AIRFLOW-2266

I suggest you to create a Python2 Virtual Environment using the following to install `apache-airflow[gcp_api]`:

```bash
virtualenv -p python2 airflow_workshop
```
OR

```bash
virtualenv -p python2.7 airflow_workshop
```

### References:
* [Installing Airflow - Official Docs](https://airflow.incubator.apache.org/installation.html)
