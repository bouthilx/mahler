# -*- coding: utf-8 -*-
"""
:mod:`mahler.cli.schedule` -- Module executing the trials
=========================================================

.. module:: schedule
   :platform: Unix
   :synopsis: Executes the trials
"""
import argparse
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

    load_modules_parser(schedule_parser)


def load_modules_parser(main_parser):
    """Create the subparsers for the subcommands"""
    subparsers = main_parser.add_subparsers(dest='scheduler', help='sub-command help')

    plugins = {
        entry_point.name: entry_point.load()
        for entry_point in pkg_resources.iter_entry_points('Scheduler')
    }

    load_modules_init_parser(subparsers, plugins)
    load_modules_run_parser(subparsers, plugins)
    load_modules_info_parser(subparsers, plugins)
    load_modules_submit_parser(subparsers, plugins)


def load_modules_init_parser(subparsers, plugins):
    """Create the subparsers for the subcommands"""
    init_parser = subparsers.add_parser('init', help='scheduler init help')

    init_parser.set_defaults(func=init)

    init_subparsers = init_parser.add_subparsers(dest='init', help='sub-command help')

    for scheduler_name, scheduler_module in plugins.items():
        scheduler_module.build_init_parser(init_subparsers)


def load_modules_run_parser(subparsers, plugins):
    """Create the subparsers for the subcommands"""
    run_parser = subparsers.add_parser('run', help='scheduler run help')

    run_parser.set_defaults(func=run)

    run_subparsers = run_parser.add_subparsers(dest='run', help='sub-command help')

    for scheduler_name, scheduler_module in plugins.items():
        run_subparser = scheduler_module.build_run_parser(run_subparsers)
        run_subparser.add_argument('commandline', nargs=argparse.REMAINDER)


def load_modules_info_parser(subparsers, plugins):
    """Create the subparsers for the subcommands"""
    info_parser = subparsers.add_parser('info', help='scheduler info help')

    info_parser.set_defaults(func=info)

    info_subparsers = info_parser.add_subparsers(dest='info', help='sub-command help')

    for scheduler_name, scheduler_module in plugins.items():
        scheduler_module.build_info_parser(info_subparsers)


def load_modules_submit_parser(subparsers, plugins):
    """Create the subparsers for the subcommands"""
    submit_parser = subparsers.add_parser('submit', help='scheduler submit help')

    submit_parser.set_defaults(func=main)

    submit_subparsers = submit_parser.add_subparsers(dest='submit', help='sub-command help')

    for scheduler_name, scheduler_module in plugins.items():
        parser = scheduler_module.build_submit_parser(submit_subparsers)
        parser.add_argument(
            '--tags', nargs='*', help='tags to select tasks to schedule')

        parser.add_argument(
            '--container', help='container used to execute the workers')

        # TODO: Should the working directory be task-specific? 
        parser.add_argument(
            '--working-dir', help='Working directory of the workers')


def init(args):
    logger.debug('arguments: {}'.format(pprint.pformat(args)))
    ressources = mahler.core.resources.build(type=args['init'], **args)
    ressources.init()

def run(args):
    logger.debug('arguments: {}'.format(pprint.pformat(args)))
    ressources = mahler.core.resources.build(type=args['run'], **args)
    ressources.run(args['commandline'])


def info(args):
    logger.debug('arguments: {}'.format(pprint.pformat(args)))
    ressources = mahler.core.resources.build(type=args['info'], **args)
    print(ressources.info())


def main(args):
    logger.debug('arguments: {}'.format(pprint.pformat(args)))
    tags = args['tags']
    container = args['container']
    working_dir = args['working_dir']
    registrar = mahler.core.registrar.build()
    ressources = mahler.core.resources.build(type=args['submit'], **args)
    logger.info('Retrieving tasks')
    task_documents = registrar.retrieve_tasks(
        tags=tags, container=container, status=mahler.core.status.Queued(''),
        _return_doc=True, _projection={'registry.container': 1, 'facility.resources': 1})

    task_per_container = dict()
    for task_document in task_documents:
        container = task_document.get('registry', {}).get('container', None)
        if container not in task_per_container:
            task_per_container[container] = []

        task_per_container[container].append(task_document)

    logger.info(
        '{} tasks available:\n{}'.format(
            sum(len(task_docs) for task_docs in task_per_container.values()),
            "\n".join(
                '{}: {}'.format(tasks_container, len(task_docs))
                for tasks_container, task_docs in task_per_container.items())))

    if not task_per_container:
        return

    any_container = task_per_container.pop(None, [])

    if not task_per_container:
        ressources.submit(any_container, tags=tags)
        return

    # TODO: Divide fair share between containers. If first workers are all on the same container
    #       they may quickly exhaust the queue for this specific container and we will end up
    #       with no workers.
    for tasks_container, task_docs in task_per_container.items():
        available_resources = ressources.available()
        logger.info('{} nodes available'.format(available_resources))
        if available_resources:
            ressources.submit(task_docs + any_container, container=tasks_container, tags=tags,
                              working_dir=working_dir)
