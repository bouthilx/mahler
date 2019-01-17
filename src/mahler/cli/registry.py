# -*- coding: utf-8 -*-
"""
:mod:`mahler.cli.registry` -- Module for registry cli
=====================================================

.. module:: registry
   :platform: Unix
   :synopsis: Registers registry commands
"""
import logging
import pkg_resources
import pprint

import mahler.core.registrar
import mahler.core.resources
import mahler.core.status
import mahler.core.worker
import mahler.core.maintainer


# TODO: 
#    current format is 
#    mahler registry init mongodb
#   
#    We should find a way to turn this around to
#    mahler registry mongodb init
#    While still making sure every registry has all the required commands
#    (right now there is only init)


logger = logging.getLogger(__name__)


def build(parser):
    """Return the parser that needs to be used for this command"""
    registry_parser = parser.add_parser('registry', help='registry help')

    load_modules_parser(registry_parser)

    registry_parser.set_defaults(func=main)


def load_modules_parser(main_parser):
    """Create the subparsers for the subcommands"""
    subparsers = main_parser.add_subparsers(dest='registry', help='sub-command help')

    plugins = {
        entry_point.name: entry_point.load()
        for entry_point in pkg_resources.iter_entry_points('RegistryDB')
    }

    load_modules_init_parser(subparsers, plugins)

    add_maintainer_parser(subparsers)


def add_maintainer_parser(subparsers):
    maintainer_parser = subparsers.add_parser('maintain', help='maintainer help')

    maintainer_parser.add_argument(
        '--types', nargs='*', choices=mahler.core.maintainer.TYPES, type=str,
        help='Types of maintenance to execute.')

    maintainer_parser.add_argument(
        '--tags', nargs='*', type=str,
        help='Tags to select for maintenance')

    maintainer_parser.add_argument(
        '--container', type=str,
        help='Container to select for maintenance')

    maintainer_parser.add_argument(
        '--sleep', type=int, default=20,
        help='Time to sleep if no tasks found to update. Default: 20')

    maintainer_parser.set_defaults(subfunc=maintain)
    

def load_modules_init_parser(subparsers, plugins):
    init_parser = subparsers.add_parser('init', help='mongodb_init help')
    init_subparsers = init_parser.add_subparsers(dest='init', help='sub-command help')

    for registry_name, registry_module in plugins.items():
        registry_module.build_init_parser(init_subparsers)


def main(args):
    # TODO Support setting name
    registrar = mahler.core.registrar.build(name='mongodb')
    args.pop('subfunc')(registrar, **args)


def maintain(registrar, **kwargs):
    mahler.core.maintainer.maintain(registrar, **kwargs)
