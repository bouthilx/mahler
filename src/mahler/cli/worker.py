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
        '--num-workers', type=int,
        help=('number of workers to spawn. Default: number of avail cpus '
              '(taking into account slurm resource assignement when relevant)'))

    execute_parser.add_argument(
        '--working-dir', help='Working directory')

    execute_parser.add_argument(
        '--max-tasks', default=int(10e10), type=int,
        help='Maximum number of tasks to execute. Default: practicaly inf (10e10)')

    execute_parser.add_argument(
        '--max-failedover-attempts', default=3, type=int,
        help=('Number of times to try again the execution of a broken task. Default: 3'))

    execute_parser.add_argument(
        '--depletion-patience', default=10, type=int,
        help=('When there is no task available, number of times to try again maintenance of the '
              'tasks before giving up and terminating the worker. Default: 10'))

    execute_parser.add_argument(
        '--exhaust-wait-time', default=20, type=float,
        help=('Number of seconds to wait between two maintenance attempts. Default: 20'))

    execute_parser.set_defaults(func=main)


def main(args):
    mahler.core.worker.start(**args)
