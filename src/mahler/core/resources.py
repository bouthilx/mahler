# -*- coding: utf-8 -*-
"""
:mod:`mahler.core.resources` -- Abstract class for scheduler plugins
====================================================================

.. module:: init_only
   :platform: Unix
   :synopsis: Defines the abstract class for scheduler plugins
"""
import pkg_resources
import pprint

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

    scheduler_type = kwargs.get('type', config.scheduler.type)

    load_config(type=scheduler_type)

    if scheduler_type is None:
        raise ValueError(
            "No type provided to build a scheduler:\n{}".format(pprint.pformat(kwargs)))

    for key in list(kwargs.keys()):
        if kwargs[key] is None:
            kwargs.pop(key)

    scheduler_config = config.scheduler[scheduler_type].to_dict()
    scheduler_config.update(kwargs)
    
    return plugins[scheduler_type].load().build(**scheduler_config)


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

    scheduler_type = kwargs.get('type', config.scheduler.type)

    if scheduler_type is None or scheduler_type == 'None':
        raise ValueError(
            "No type provided to build a scheduler:\n{}".format(pprint.pformat(kwargs)))

    plugin = plugins[scheduler_type].load()

    config.scheduler[scheduler_type] = plugin.define_config()
    plugin.parse_config_files(config.scheduler[scheduler_type])
