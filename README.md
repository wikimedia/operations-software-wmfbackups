Collection of Python classes and scripts to operate with MariaDB servers.

## Dependencies

Some dependencies are required in order to run the scripts and the tests. The easiest way to work is by using a virtualenv.

```
virtualenv --no-site-packages -p python3.5 venv
. venv/bin/activate
pip install -U -r requirements.txt -r test-requirements.txt
```

## Run tests

Test are located under *wmfmariadbpy/test*. To run the tests and get a coverage report:

```
nosetests --with-coverage --cover-package wmfmariadbpy
```

### Integration tests requirements

In order to be able to to run the tests you'll need to be able to run the script localy. You'll need to have:
* A *.my.cnf* file with the proper conf
* A MariaDB listening on localhost:3306
* *pt-online-schema-change* script on your PATH

## Code style compliance

To check the code style compliance:

```
flake8 --exclude wmfmariadbpy/check_health.py,wmfmariadbpy/compare.py wmfmariadbpy
```

Right now we are excluding those two files, until they are improved.

## Execution

Only *osc_host.py* is included on the setup for now, so the rest of them can be run directly. As for *osc_host.py*, the setup script needs to be run first:

```
python setup.py develop
```

Once that is done, we can run the script. For instance:

```
osc_host --method=ddl --host=localhost --db=test --table=test "add column test int"
```
