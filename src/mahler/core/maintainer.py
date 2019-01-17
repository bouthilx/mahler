import logging
import random
import time


logger = logging.getLogger(__name__)


TYPES = ['unreported', 'to_queue', 'onhold', 'lost', 'reports', 'broken']


def maintain(registrar, *args, **kwargs):
    types = kwargs['types']
    tags = kwargs['tags']
    container = kwargs['container']
    sleep_time = kwargs['sleep']

    while True:
        updated = 0
        for maintaining_type in types:
            if maintaining_type != 'unreported':
                type_updated = getattr(registrar, "maintain_{}".format(maintaining_type))(
                    tags=tags, container=container, limit=None)
            else:
                type_updated = getattr(registrar, "maintain_{}".format(maintaining_type))(limit=None)

            logger.info("    {: 3d} tasks updated for type {}".format(type_updated, maintaining_type))

            updated += type_updated

        if updated:
            logger.info("{} tasks updated in total".format(updated))
        elif sleep_time:
            sleep_time = random.gauss(sleep_time, sleep_time / 10.)
            logger.info(
                'No more task to maintain. Waiting {}s before trying again.'.format(sleep_time))
            time.sleep(sleep_time)
