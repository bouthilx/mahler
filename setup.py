#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Installation script for Mahler."""
from glob import iglob
import os
import sys

from setuptools import setup, find_namespace_packages

import versioneer

repo_root = os.path.dirname(os.path.abspath(__file__))

tests_require = [
    'pytest>=3.0.0'
    ]


setup_args = dict(
    name='mahler.core',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='',
    long_description=open(os.path.join(repo_root, "README.rst")).read(),
    license='GNU GPLv3',
    author=u'Xavier Bouthillier',
    author_email='xavier.bouthillier@umontreal.ca',
    url='https://github.com/bouthilx/mahler',
    packages=find_namespace_packages(where='src'),
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=['PyYAML', 'appdirs', 'tblib', 'cotyledon', 'hurry.filesize', 'lxml', 'psutil'],
    tests_require=tests_require,
    setup_requires=['setuptools>=v40.1.0', 'appdirs', 'pytest-runner>=2.0,<3dev'],
    extras_require=dict(test=tests_require),
    entry_points={
        'console_scripts': [
            'mahler = mahler.cli.main:main'
        ],
        'RegistryDB': [
        ],
        'Scheduler': [
        ],
        'Dashboard': [
        ],
        'cli': [
            'schedule = mahler.cli.schedule:build',
            'worker = mahler.cli.worker:build',
            'dashboard = mahler.cli.dashboard:build',
            'registry = mahler.cli.registry:build'
        ],
    },
    # "Zipped eggs don't play nicely with namespace packaging"
    # from https://github.com/pypa/sample-namespace-packages
    zip_safe=False
    )

setup_args['keywords'] = [
    ]

setup_args['platforms'] = ['Linux']

setup_args['classifiers'] = [
    'Development Status :: 1 - Planning',
    'Intended Audience :: Developers',
    'Intended Audience :: Education',
    'Intended Audience :: Science/Research',
    'License :: OSI Approved :: GPU GPLv3',
    'Operating System :: POSIX',
    'Operating System :: Unix',
    'Programming Language :: Python',
    'Topic :: Scientific/Engineering',
] + [('Programming Language :: Python :: %s' % x)
     for x in '3 3.5 3.6 3.7'.split()]

if __name__ == '__main__':
    setup(**setup_args)
