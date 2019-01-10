# -*- coding: utf-8 -*-
"""
"""
import os

from appdirs import AppDirs

from mahler.core.utils.config import Configuration, parse_config_files

from ._version import get_versions


VERSIONS = get_versions()
del get_versions

__descr__ = 'Orchestration engine'
__version__ = VERSIONS['version']
__license__ = 'GNU GPLv3'
__author__ = u'Xavier Bouthillier'
__author_short__ = u'Xavier Bouthillier'
__author_email__ = 'xavier.bouthillier@umontreal.ca'
__copyright__ = u'2018-2019, Xavier Bouthillier'
__url__ = 'https://github.com/bouthilx/mahler'

DIRS = AppDirs('mahler', __author_short__)
del AppDirs

DEF_CONFIG_FILES_PATHS = [
    os.path.join(DIRS.site_data_dir, 'config.yaml.example'),
    os.path.join(DIRS.site_config_dir, 'config.yaml'),
    os.path.join(DIRS.user_config_dir, 'config.yaml')
    ]

config = Configuration()
config.add_option(
    'heartbeat', type=int, default=60, env_var='MAHLER_HEARTBEAT')

config.registry = Configuration()
config.registry.add_option(
    'type', type=str, default=None, env_var='MAHLER_REGISTRAR_TYPE')

config.scheduler = Configuration()
config.scheduler.add_option(
    'type', type=str, default=None, env_var='MAHLER_SCHEDULER_TYPE')

parse_config_files(config, DEF_CONFIG_FILES_PATHS)
