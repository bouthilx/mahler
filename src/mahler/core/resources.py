# -*- coding: utf-8 -*-
"""
:mod:`mahler.core.resources` -- Abstract class for scheduler plugins
====================================================================

.. module:: init_only
   :platform: Unix
   :synopsis: Defines the abstract class for scheduler plugins
"""
import pkg_resources

from mahler.core import config


class Resources(object):
    def __init__(self):
        pass

    def available(self):
        raise NotImplementedError()

    def submit(self, number_of_processes):
        raise NotImplementedError()


def build(**kwargs):

    plugins = {
        entry_point.name: entry_point
        for entry_point
        in pkg_resources.iter_entry_points('Scheduler')
    }

    scheduler_type = kwargs.get('type', mahler.core.config.scheduler.type)

    load_config(type=scheduler_type)

    if scheduler_type is None:
        raise ValueError(
            "No type provided to build a scheduler:\n{}".format(pprint.pformat(kwargs)))

    config = mahler.core.config.scheduler[scheduler_type].to_dict()
    config.update(kwargs)
    
    return plugins[scheduler_type].load().build(**config)


def load_config(**kwargs):
    """
    Note
    ----
        A plugin may only define and modify its own sub-configuration.
        It should not modify the configuration of the core or other plugins
    """

    plugins = {
        entry_point.name: entry_point
        for entry_point
        in pkg_resources.iter_entry_points('Scheduler')
    }

    scheduler_type = kwargs.get('type', mahler.core.config.scheduler.type)

    if scheduler_type is None or scheduler_type == 'None':
        raise ValueError(
            "No type provided to build a scheduler:\n{}".format(pprint.pformat(kwargs)))

    plugin = plugins[scheduler_type].load()

    mahler.core.config.scheduler[scheduler_type] = plugin.define_config()
    plugin.parse_config_files(mahler.core.config.scheduler[scheduler_type])
