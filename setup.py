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
        'pymysql',
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
            'osc_host = wmfmariadbpy.osc_host:main',
        ],
    },
    test_suite='wmfmariadbpy.test',
)
