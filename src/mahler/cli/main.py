# -*- coding: utf-8 -*-
"""
:mod:`mahler.cli` -- Functions that define console scripts
================================================================

.. module:: cli
   :platform: Unix
   :synopsis: Helper functions to setup a study, execute it and analyze it.

"""
import argparse
import logging
import pkg_resources

import mahler.core

log = logging.getLogger(__name__)


def load_modules_parser(main_parser):
    """Create the subparsers for the subcommands"""
    subparsers = main_parser.add_subparsers(help='sub-command help')

    plugins = {
        entry_point.name: entry_point
        for entry_point in pkg_resources.iter_entry_points('cli')
    }

    for command_name, command_factory in plugins.items():
        if command_name == "dashboard":
            continue
        command_factory.load()(subparsers)


def build_main_parser():
    parser = argparse.ArgumentParser(description='Laboratorium')

    parser.add_argument(
        '-V', '--version',
        action='version', version='mahler ' + mahler.core.__version__)

    parser.add_argument(
        '-v', '--verbose',
        action='count', default=0,
        help="logging levels of information about the process (-v: INFO. -vv: DEBUG)")

    parser.add_argument(
        '-d', '--debug', action='store_true',
        help="To define...")

    return parser


def main(argv=None):
    """Main parser for the cli of `mahler`"""
    main_parser = build_main_parser()

    load_modules_parser(main_parser)

    args = vars(main_parser.parse_args(argv))

    verbose = args.pop('verbose', 0)
    levels = {0: logging.WARNING,
              1: logging.INFO,
              2: logging.DEBUG}
    logging.basicConfig(level=levels.get(verbose, logging.DEBUG))

    function = args.pop('func')
    function(args)
