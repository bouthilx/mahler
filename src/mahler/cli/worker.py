# -*- coding: utf-8 -*-
"""
:mod:`mahler.cli.execute` -- Module executing the trials
==============================================================

.. module:: execute
   :platform: Unix
   :synopsis: Executes the trials
"""
import logging

import mahler.core.worker


log = logging.getLogger(__name__)


def build(parser):
    """Return the parser that needs to be used for this command"""
    execute_parser = parser.add_parser('execute', help='execute help')

    execute_parser.add_argument('--tags', nargs='*', help='tags to select tasks for execution')

    execute_parser.set_defaults(func=main)


def main(args):
    mahler.core.worker.main(tags=args['tags'])
