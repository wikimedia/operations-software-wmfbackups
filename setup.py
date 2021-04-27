'''wmfbackups.'''
from setuptools import setup

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='wmfbackups',
    description='wmfbackups',
    version='0.5',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://phabricator.wikimedia.org/diffusion/OSWB/",
    packages=['wmfbackups'],
    install_requires=['pymysql>=0.9.3',
                      'wmfmariadbpy @ git+https://gerrit.wikimedia.org/r/operations/software/wmfmariadbpy@v0.6'],
    entry_points={
        'console_scripts': [
           # cli
           'backup-mariadb = wmfbackups.cli.backup_mariadb:main',
           'recover-dump = wmfbackups.cli.recover_dump:main',
           # cli_remote
           'remote-backup-mariadb = wmfbackups.cli_remote.remote_backup_mariadb:main',
           # check
           'check-mariadb-backups = wmfbackups.check.check_mariadb_backups:main'
        ]
    },
    test_suite='wmfbackups.test',
)
