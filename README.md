Collection of Python classes and scripts to operate with MariaDB servers.

## Dependencies

Some dependencies are required in order to run the scripts and the tests. The easiest way to work is by using a virtualenv.

```
virtualenv -p python3.6 venv
. venv/bin/activate
pip install -U -r requirements.txt -r test-requirements.txt
```

## Run tests

Test are located under *wmfmariadbpy/test*. To run the tests and get a coverage report:

```
nosetests --with-coverage --cover-package wmfmariadbpy
```

To check the code style compliance:

```
flake8 wmfmariadbpy/osc_host.py wmfmariadbpy/test/
```

## Execution

Only *osc_host.py* is included on the setup for now, so the rest of them can be run directly. As for *osc_host.py*, the setup script needs to be run first:

```
python setup.py develop
```

Once that is done, we can run the script. For instance:

```
osc_host --method=ddl --host=localhost --db=test --table=test "add column test int"
```
