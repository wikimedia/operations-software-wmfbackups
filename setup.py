"""wmfmariadbpy."""
import os
from setuptools import setup

cwd = os.path.dirname(__file__)

setup(
    name='wmfmariadbpy',
    description='wmfmariadbpy',
    version='0.1',
    url='https://phabricator.wikimedia.org/diffusion/OSMD/',
    packages=(
        'wmfmariadbpy',
    ),
    install_requires=open(os.path.join(cwd, 'requirements.txt')).readlines(),
    tests_require=open(os.path.join(cwd, 'test-requirements.txt')).readlines(),
    entry_points={
        # TODO: Expand
        'console_scripts': [
            'osc_host = wmfmariadbpy.osc_host:main',
        ],
    },
    test_suite='wmfmariadbpy.test',
)
