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

    execute_parser.add_argument(
        '--tags', nargs='*', help='tags to select tasks for execution')

    execute_parser.add_argument(
        '--container', help='container used to execute the worker')

    execute_parser.add_argument(
        '--working-dir', help='Working directory')

    execute_parser.add_argument(
        '--max-tasks', default=int(10e10), type=int,
        help='Maximum number of tasks to execute. Default: practicaly inf (10e10)')

    execute_parser.add_argument(
        '--max-maintain', default=10, type=int,
        help=('Maximum number of tasks to maintaint when no more tasks available. '
              'Maintaining many tasks increases the delay between executions but improves '
              'the throughput when there is many workers. Default: 10'))

    execute_parser.add_argument(
        '--depletion-patience', default=12, type=int,
        help=('When there is no task available, number of times to try again maintenance of the '
              'tasks before giving up and terminating the worker. Default is 12'))

    execute_parser.add_argument(
        '--exhaust-wait-time', default=10, type=float,
        help=('Number of seconds to wait between two maintenance attempts. Default: 10'))

    execute_parser.set_defaults(func=main)


def main(args):
    mahler.core.worker.main(**args)
