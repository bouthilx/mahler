# -*- coding: utf-8 -*-
"""
:mod:`mahler.cli.dashboard` -- Module creating and running a dashboard
======================================================================

.. module:: dashboard
   :platform: Unix
   :synopsis: TODO

TODO
"""
import logging

import mahler.core.worker

from .monitor import app


log = logging.getLogger(__name__)


def build(parser):
    """Return the parser that needs to be used for this command"""
    dashboard_parser = parser.add_parser('dashboard', help='dashboard help')

    dashboard_parser.set_defaults(func=main)

    load_modules_parser(dashboard_parser)


def load_modules_parser(main_parser):
    """Create the subparsers for the subcommands"""
    subparsers = main_parser.add_subparsers(dest='dashboard', help='sub-command help')

    plugins = {
        entry_point.name: entry_point
        for entry_point in pkg_resources.iter_entry_points('Dashboard')
    }

    for scheduler_name, scheduler_module in plugins.items():
        scheduler_module.load().build_parser(subparsers)
