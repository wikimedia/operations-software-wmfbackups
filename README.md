Collection of Python classes and scripts to create and manage wmf database and other backups.

## Dependencies

Some dependencies are required in order to run the scripts and the tests. The easiest way to work is by using a virtualenv:

```
tox --notest
tox -e venv -- <some command>
```

## Run tests

Tests are located under *wmfbackups/test*. They are split between unit and integration tests. To run unit tests:

```
tox -e unit
```

### Integration tests requirements

In order to be able to to run the tests you'll need to be able to run the script localy. You'll need to have:
* A *.my.cnf* file with the proper configuration
* A MariaDB listening on localhost:3306

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

## Packaging

To create debian packages:

```
debuild -b -us -uc
```

## Database creation

To create the database objects and users to track/check the metadata:

```
mysql -e "CREATE DATABASE dbbackups DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
mysql dbbackups < sql/dbbackups.sql
# user to setup an icinga check
mysql -e "CREATE USER checkuser IDENTIFIED BY 'password' WITH MAX_USER_CONNECTIONS 10; GRANT SELECT ON dbbackups.* TO checkuser"
# user to update the backup metadata
mysql -e "CREATE USER statsuser IDENTIFIED BY 'another_password'; GRANT SELECT, INSERT, UPDATE ON dbbackups.* TO statsuser"
```

Adapt your mysql connection properties, passwords, database name and user accounts, that you later will configure at /etc/wmfbackups/statistics.cnf and nagios options.
