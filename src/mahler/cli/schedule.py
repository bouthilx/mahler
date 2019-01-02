# -*- coding: utf-8 -*-
"""
:mod:`mahler.cli.schedule` -- Module executing the trials
=========================================================

.. module:: schedule
   :platform: Unix
   :synopsis: Executes the trials
"""
import logging
import pkg_resources

import mahler.core.registrar
import mahler.core.resources
import mahler.core.status
import mahler.core.worker


log = logging.getLogger(__name__)


def build(parser):
    """Return the parser that needs to be used for this command"""
    schedule_parser = parser.add_parser('schedule', help='schedule help')

    load_modules_parser(schedule_parser)

    schedule_parser.set_defaults(func=main)


def load_modules_parser(main_parser):
    """Create the subparsers for the subcommands"""
    subparsers = main_parser.add_subparsers(dest='scheduler', help='sub-command help')

    plugins = {
        entry_point.name: entry_point.load()
        for entry_point in pkg_resources.iter_entry_points('Scheduler')
    }

    for scheduler_name, scheduler_module in plugins.items():
        scheduler_module.build_parser(subparsers)


def main(args):
    registrar = mahler.core.registrar.build(name=args.get('registrar', 'mongodb'))
    ressources = mahler.core.resources.build(name=args['scheduler'], **args)
    tags = ['examples', 'random', 'flow', 'v1.0']
    registrar.maintain(tags)
    tasks = registrar.retrieve_tasks(tags, status=mahler.core.status.Queued(''))
    task_per_container = dict()
    for task in tasks:
        container = task.container if task.container else None
        if container not in task_per_container:
            task_per_container[container] = []

        task_per_container[container].append(task)

    if not task_per_container:
        return

    any_container = task_per_container.pop(None, [])

    if not task_per_container:
        ressources.submit(any_container, tags=tags)
        return

    # TODO: Divide fair share between containers. If first workers are all on the same container
    #       they may quickly exhaust the queue for this specific container and we will end up
    #       with no workers.
    for container, tasks in task_per_container.items():
        if ressources.available():
            ressources.submit(tasks + any_container, container=container, tags=tags)
