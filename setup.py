"""wmfmariadbpy."""
from setuptools import setup

setup(
    name='wmfmariadbpy',
    description='wmfmariadbpy',
    version='0.1',
    url='https://phabricator.wikimedia.org/diffusion/OSMD/',
    packages=(
        'wmfmariadbpy',
    ),
    install_requires=[
        'pymysql>=0.9.3',
        'tabulate',
        'cumin'
    ],
    tests_require=[
        'flake8',
        'nose',
        'coverage',
    ],
    entry_points={
        # TODO: Expand
        'console_scripts': [
            'db-compare = wmfmariadbpy.compare:main',
            'mysql.py = wmfmariadbpy.mysql:main',
            'db-osc-host = wmfmariadbpy.osc_host:main',
            'db-switchover = wmfmariadbpy.switchover:main',
            'db-replication-tree = wmfmariadbpy.replication_tree:main',
            'db-move-replica = wmfmariadbpy.move_replica:main',
            'db-stop-in-sync = wmfmariadbpy.stop_in_sync:main',
        ],
    },
    test_suite='wmfmariadbpy.test',
)
