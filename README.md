Collection of Python classes and scripts to operate with MariaDB servers.

## Dependencies

Some dependencies are required in order to run the scripts and the tests. The easiest way to work is by using a virtualenv:

```
tox --no-test
tox -e venv -- <some command>
```

## Run tests

Tests are located under *wmfmariadbpy/test*. They are split between unit and integration tests. To run unit tests:

```
tox -e unit
```

### Integration tests requirements

In order to be able to to run the tests you'll need to be able to run the script localy. You'll need to have:
* A *.my.cnf* file with the proper configuration
* A MariaDB listening on localhost:3306
* *pt-online-schema-change* script on your PATH

Then:
```
tox -e integration
```

### Tests coverage report

To run the unit and integration tests and generate a HTML coverage report under `cover/`

```
tox -e cover
```

## Code style compliance

To check the code style compliance:

```
tox -e flake8
```

## Execution

Only *osc_host.py* is included on the setup for now, so the rest of them can be run directly. As for *osc_host.py* the easiest is to run it via the virtualenv `venv`:
```
tox -e venv -- osc_host --method=ddl --host=localhost --db=test --table=test "add column test int"
```
