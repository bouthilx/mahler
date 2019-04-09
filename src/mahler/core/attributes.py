import copy
import datetime


class EventBasedAttribute(object):
    """
    {
        _id:
        task_id:
        inc_id:
        key:
        runtime_timestamp:
        item:{
        }
    }
    """
    def __init__(self, task, name):
        self.task = task
        self.name = name
        self.timestamp = None
        self.history = []
        self.last_item = None

    def __iter__(self):
        return iter(self.history)

    def __getitem__(self, key):
        return self.history[key]

    def replay(self):
        raise NotImplemented()

    @property
    def events(self):
        self.refresh(full=True)
        return self.history

    @property
    def value(self):
        raise NotImplementedError()
        # TODO: Cache replay for some time...
        self.refresh()
        return self.replay()

    # TODO: Indexes in mongodb
    # def _setup_db(self):
    #     if self.collection_name not in EventBasedAttributeWithDB.indexes_built:
    #         try:
    #             self._db.ensure_index(self.collection_name, 'trial_id')
    #             self._db.ensure_index(self.collection_name, 'runtime_timestamp')
    #             self._db.ensure_index(self.collection_name, 'creation_timestamp')
    #         except BaseException as e:
    #             if "not authorized on" not in str(e):
    #                 raise

    #         EventBasedAttributeWithDB.indexes_built.add(self.collection_name)

    @property
    def last_id(self):
        if not self.last_item:
            return 0

        return int(self.last_item['inc_id'])

    # TODO: Have an option to just fetch the last index instead of the whole history.
    #       When just appending or just looking at last value, we don't need the whole thing.
    def refresh(self, full=False):
        if self.task._registrar:
            # self.history = self.task._registrar.retrieve_status(self.task)
            if full:
                events = list(self.task._registrar._db.retrieve_events(
                    self.name, self.task,
                    updated_after=str(self.history[-1]['id']) if self.history else None))
                self.history = list(sorted(self.history + events, key=lambda event: event['inc_id']))


                if self.history:
                    self.last_item = self.history[-1]
            else:
                events = list(self.task._registrar._db.retrieve_events(
                    self.name, self.task,
                    limit=3, sort=[('_id', -1)],
                    updated_after=str(self.last_item['id']) if self.last_item else None))

                if events:
                    self.last_item = list(sorted(
                        events,
                        key=lambda event: event['inc_id']))[-1]


    def append_event(self, event):
        if self.history or (not self.history and not self.last_item):
            self.history.append(event)
        self.last_item = event

        # query = {"trial_id": self._trial_id}
        # lower_bound, upper_bound = self._interval
        # if (self.history and
        #         (lower_bound is None or lower_bound < self.history[-1]['runtime_timestamp'])):
        #     lower_bound = self.history[-1]['runtime_timestamp']

        # # Can't query anything anymore
        # if lower_bound and upper_bound and lower_bound > upper_bound:
        #     return {}

        # if lower_bound:
        #     query['runtime_timestamp'] = {'$gte': lower_bound}
        # elif upper_bound:
        #     query['runtime_timestamp'] = {'$lte': upper_bound}

        # new_events = self._db.read(self.collection_name, query)

        # if self._interval[0] and self._interval[1]:
        #     new_events = [event for event
        #                   in new_events if event['runtime_timestamp'] <= upper_bound]

        # self.history += self._filter_duplicates(new_events)

        # return self

    def _filter_duplicates(self, new_events):
        if not self.history:
            return new_events

        last_id = self.last_id

        return [e for e in new_events if int(e['id'].split(".")[-1]) > last_id]

    # def _save(self, event):
    #     # Make sure we have full history
    #     event['id'] = "{}.{}".format(self._trial_id, self.last_id + 1)
    #     self._db.write(self.collection_name, event


class EventBasedListAttribute(EventBasedAttribute):
    ADD = "add"
    REMOVE = "remove"

    @property
    def value(self):
        return self.replay()

    def replay(self):
        items = []
        for event in self.events:
            if event['type'] == self.ADD:
                items.append(event['item'])
            elif event['type'] == self.REMOVE:
                del items[items.index(event['item'])]
            else:
                raise ValueError("WTF")
                    # "Invalid event type '{}', must be '{}' or '{}'".format(
                    #     (event['type'], self.ADD, self.REMOVE)))

        return items

    def append(self, new_item, timestamp=None, creator=None):
        self.register_event(self.ADD, new_item, timestamp=timestamp, creator=creator)

    def remove(self, item, timestamp=None, creator=None):
        if item not in self.replay():
            raise RuntimeError(
                "Cannot remove item that is not in the list:\n{}".format(item))

        self.register_event(self.REMOVE, item, timestamp=timestamp, creator=creator)


class EventBasedFileAttribute(EventBasedAttribute):
    ADD = "add"

    # TODO: Setup in mongodb
    # def _setup_db(self):
    #     if self.collection_name not in EventBasedAttributeWithDB.indexes_built:
    #         try:
    #             self._db.ensure_index(self.collection_name + ".metadata", 'trial_id')
    #             self._db.ensure_index(self.collection_name + ".metadata", 'filename')
    #             self._db.ensure_index(self.collection_name + ".metadata", 'runtime_timestamp')
    #             self._db.ensure_index(self.collection_name + ".metadata", 'creation_timestamp')
    #             # Because we are not using gridfs for now...
    #             self._db.ensure_index(self.collection_name, 'trial_id')
    #             self._db.ensure_index(self.collection_name, 'filename')
    #             self._db.ensure_index(self.collection_name, 'runtime_timestamp')
    #             self._db.ensure_index(self.collection_name, 'creation_timestamp')
    #         except BaseException as e:
    #             if "not authorized on" not in str(e):
    #                 raise

    #         EventBasedAttributeWithDB.indexes_built.add(self.collection_name)

    def replay(self):
        items = []
        for event in self.events:
            if event['type'] == self.ADD:
                items.append(event['item'])
            elif event['type'] == self.REMOVE:
                del items[items.find(event['item'])]
            else:
                raise ValueError(
                    "Invalid event type '{}', must be '{}' or '{}'".format(
                        (event['type'], self.ADD, self.REMOVE)))

        return items

    # def register_event(self, event_type, item, timestamp=None, creator=None):
    #     file_like_object = item.pop('file_like_object')
    #     event = self.create_event(event_type, item, timestamp=timestamp, creator=creator)
    #     event['trial_id'] = self._trial_id
    #     self._save(event, file_like_object)
    #     self.history.append(event)

    # def _save(self, event, file_like_object):
    #     # Make sure we have full history
    #     event['_id'] = "{}.{}".format(self._trial_id, self.last_id + 1)
    #     metadata = copy.deepcopy(event['item'])
    #     event.pop('item')
    #     metadata.update(event)
    #     file_id = self._db.write_file(self.collection_name, file_like_object, metadata=metadata)
    #     event['item'] = metadata
    #     event['item']['file_id'] = file_id
    #     self._db.write(self.collection_name, event)

    # def add(self, filename, file_like_object, attributes, timestamp=None, creator=None):
    #     attributes['filename'] = filename
    #     attributes['file_like_object'] = file_like_object
    #     self.register_event(self.ADD, attributes, timestamp=timestamp, creator=creator)

    def get(self, filename, query):
        query = copy.deepcopy(query)
        query['trial_id'] = self._trial_id
        lower_bound, upper_bound = self._interval

        if lower_bound:
            query['runtime_timestamp'] = {'$gte': lower_bound}
        elif upper_bound:
            query['runtime_timestamp'] = {'$lte': upper_bound}

        query['filename'] = filename

        files = self._db.read_file(self.collection_name, query)

        if lower_bound and upper_bound:
            files = [f for f, metadata in files if metadata['runtime_timestamp'] <= upper_bound]

        return files


class EventBasedItemAttribute(EventBasedAttribute):
    SET = "set"

    @property
    def value(self):
        self.refresh(full=False)
        return self.replay()

    def replay(self):
        if self.last_item:
            return self.last_item['item']

        return None

    # def set(self, new_item, timestamp=None, creator=None):
    #     self.register_event(self.SET, new_item, timestamp=timestamp, creator=creator)
