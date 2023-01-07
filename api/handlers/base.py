from queue import Queue


class QueueMixin:
    def __init__(self):
        self._queue = Queue()

    @property
    def queue(self):
        return self._queue.get().result()

    @queue.setter
    def queue(self, task):
        self._queue.put(task)
