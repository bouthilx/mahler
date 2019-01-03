import logging
import random

from mahler.client import Client


logger = logging.getLogger('ops')


mahler = Client()


tags = ['examples', 'rosenbrock', 'random', 'v1.4']


space_keys = 'xyz'


def sample():
    return random.uniform(-10, 10)


def rosenbrock(*args):

    if args[1] is None:
        return 4 * args[0]**2 + 23.4

    rval = 0
    for i in range(len(args) - 1):
        if args[i + 1] is None:
            continue
        rval += 100 * (args[i + 1] - args[i]**2)**2 - (1 - args[i])**2

    return rval


@mahler.operator()
def run(x, y, z):
    logger.info('Running run()')
    rval = rosenbrock(x, y, z)
    return dict(objective=rval), dict()


@mahler.operator()
def create_trial(container=None):
    logger.info('Get trials')
    trials = list(mahler.find(tags + [run.name]))
    logger.info('There is {} trials'.format(len(trials)))

    if len(trials) > 1000:
        return {}, {}
    
    for i in range(20):
        logger.info('Sampling')
        params = dict(zip(space_keys, (sample() for key in space_keys)))
        logger.info('Registering new run')
        trial_execution = mahler.register(run.delay(**params), tags=tags, container=container)

    for i in range(10):
        logger.info('Registering new create_trial')
        mahler.register(create_trial.delay(container=container), tags=tags, container=container)

    return {}, {}
