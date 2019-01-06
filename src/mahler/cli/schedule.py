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
import pprint

import mahler.core.registrar
import mahler.core.resources
import mahler.core.status
import mahler.core.worker


logger = logging.getLogger(__name__)


def build(parser):
    """Return the parser that needs to be used for this command"""
    schedule_parser = parser.add_parser('schedule', help='schedule help')

    schedule_parser.add_argument(
        '--tags', nargs='*', help='tags to select tasks to schedule')

    schedule_parser.add_argument(
        '--container', help='container used to execute the workers')

    # TODO: Should the working directory be task-specific? 
    schedule_parser.add_argument(
        '--working-dir', help='Working directory of the workers')

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
    logger.debug('arguments: {}'.format(pprint.pformat(args)))
    tags = args['tags']
    container = args['container']
    working_dir = args['working_dir']
    registrar = mahler.core.registrar.build()
    ressources = mahler.core.resources.build(type=args['scheduler'], **args)
    logger.info('Maintaining tags')
    registrar.maintain(tags)
    logger.info('Retrieving tasks')
    tasks = registrar.retrieve_tasks(tags=tags, container=container,
                                     status=mahler.core.status.Queued(''))
    task_per_container = dict()
    for task in tasks:
        container = task.container if task.container else None
        if container not in task_per_container:
            task_per_container[container] = []

        task_per_container[container].append(task)

    logger.info(
        '{} tasks available:\n{}'.format(
            sum(len(tasks) for tasks in task_per_container.values()),
            "\n".join(
                '{}: {}'.format(tasks_container, len(tasks))
                for tasks_container, tasks in task_per_container.items())))

    if not task_per_container:
        return

    any_container = task_per_container.pop(None, [])

    if not task_per_container:
        ressources.submit(any_container, tags=tags)
        return

    # TODO: Divide fair share between containers. If first workers are all on the same container
    #       they may quickly exhaust the queue for this specific container and we will end up
    #       with no workers.
    for tasks_container, tasks in task_per_container.items():
        available_resources = ressources.available()
        logger.info('{} nodes available'.format(available_resources))
        if available_resources:
            ressources.submit(tasks + any_container, container=tasks_container, tags=tags,
                              working_dir=working_dir)
