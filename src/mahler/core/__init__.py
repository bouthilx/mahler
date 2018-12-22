# -*- coding: utf-8 -*-
"""
"""
from appdirs import AppDirs

from ._version import get_versions

from mahler.core.utils.config import Configuration

VERSIONS = get_versions()
del get_versions

__descr__ = 'Orchestration engine'
__version__ = VERSIONS['version']
__license__ = 'GNU GPLv3'
__author__ = u'Xavier Bouthillier'
__author_short__ = u'Xavier Bouthillier'
__author_email__ = 'xavier.bouthillier@umontreal.ca'
__copyright__ = u'2018-2019, Xavier Bouthillier'
__url__ = 'https://gitlab.com/bouthilx/mahler'

DIRS = AppDirs(__name__, __author_short__)
del AppDirs

config = Configuration()

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
