'''wmfbackups.'''
from setuptools import setup

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='wmfbackups',
    description='wmfbackups',
    version='0.1',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://phabricator.wikimedia.org/diffusion/OSWB/",
    packages=['wmfbackups'],
    install_requires=['pymysql>=0.9.3',
                      'wmfmariadbpy'],  # this is supposed to be wmfmariadbpy>=0.5, but that doesn't exist yet
    extras_require={'cumin': ['cumin']},
    entry_points={
        'console_scripts': [
        ]
    },
    test_suite='wmfbackups.test',
)
