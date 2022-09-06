from threading import Event, Thread
from time import sleep
from typing import Any, Callable, List
from queue import Queue
import logging

# Utility methods for extending the functionality of threading.Event objects.
# These methods simply allow us to short-circuit multiple events, so we can
# flush the batch when either 
# 1) the "batch is full" event fires, or 
# 2) the "5 seconds have passed" event fires
# ... without having to spend CPU on constant polling for either.
#
# Credit @ SO/Claudiu

def or_set(self):
    self._set()
    self.changed()


def or_clear(self):
    self._clear()
    self.changed()


def anyfy(e, changed_callback):
    e._set = e.set
    e._clear = e.clear
    e.changed = changed_callback
    e.set = lambda: or_set(e)
    e.clear = lambda: or_clear(e)


def any_event(*events):
    or_event = Event()

    def changed():
        bools = [e.is_set() for e in events]
        if any(bools):
            or_event.set()
        else:
            or_event.clear()

    for e in events:
        anyfy(e, changed)

    changed()
    return or_event


class TimedBatchWorker():
    """
    A worker class that batches work items and flushes them to the provided
    handler whenever the batch is filled, or when the timeout is reached.
    
    :param handler: The function that will be called when the batch is flushed.
                    The batch of work items will be passed as a list.
    :param flush_interval: The interval (seconds) between which the batch is flushed.
    :param flush_batch_size: The max batch size at which the batch is flushed.
    """
    def __init__(
        self,
        handler: Callable[[List[Any]], Any],
        flush_interval=5,
        flush_batch_size=20,
    ) -> None:

        self.is_running = False

        self._queue = Queue()
        self._handler = handler
        self._flush_interval = flush_interval
        self._flush_batch_size = flush_batch_size
        self._item_enqueued_event = None

        self._timed_worker_thread = None
        self._batch_worker_thread = None
        self._flush_worker_thread = None

    def _timed_worker(self, flush_event: Event):
        while True:
            sleep(self._flush_interval)
            flush_event.set()

    def _batch_worker(self, log_created_event: Event, flush_event: Event):
        while True:
            log_created_event.wait()
            if self._queue.unfinished_tasks > self._flush_batch_size:
                flush_event.set()

            log_created_event.clear()

    def _flush_worker(self, do_flush_event: Event):
        batch = []

        while True:
            do_flush_event.wait()

            while not self._queue.empty():
                batch.append(self._queue.get())
                self._queue.task_done()

            try:
                self._handler(batch)
            except Exception as e:
                logging.error(
                    f'TimedBatchWorker encountered an error while calling handler: {e}'
                )

            batch = []
            do_flush_event.clear()

    def start(self):
        self._item_enqueued_event = Event()

        flush_timeout_event = Event()
        flush_full_batch_event = Event()

        do_flush_event = any_event(
            flush_timeout_event,
            flush_full_batch_event
        )

        self._batch_worker_thread = Thread(target=self._batch_worker, args=(
            self._item_enqueued_event, flush_full_batch_event,), daemon=True)
        self._timed_worker_thread = Thread(target=self._timed_worker, args=(
            flush_timeout_event,), daemon=True)
        self._flush_worker_thread = Thread(target=self._flush_worker, args=(
            do_flush_event,), daemon=True)

        self._timed_worker_thread.start()
        self._batch_worker_thread.start()
        self._flush_worker_thread.start()

        self.is_running = True

    def enqueue(self, item):
        self._queue.put(item)
        self._item_enqueued_event.set()
