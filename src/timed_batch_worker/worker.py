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

class InterruptableEvent(Event):
    is_interrupted = False

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


def any_event(*events: InterruptableEvent):
    or_event = InterruptableEvent()

    def changed():
        found_set_event = False
        for e in events:
            if e.is_set():
                found_set_event = True

                if e.is_interrupted:
                    or_event.is_interrupted = True

                break

        if found_set_event:
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
        self._flush_timeout_event = None
        self._flush_full_batch_event = None

        self._timed_worker_thread = None
        self._batch_worker_thread = None
        self._flush_worker_thread = None

    def _timed_worker(self, flush_event: InterruptableEvent, parent_worker: 'TimedBatchWorker'):
        while True:
            sleep(self._flush_interval)

            if not parent_worker.is_running:
                return

            flush_event.set()

    def _batch_worker(self, log_created_event: InterruptableEvent, flush_event: InterruptableEvent):
        while True:
            log_created_event.wait()

            if log_created_event.is_interrupted:
                return

            if self._queue.unfinished_tasks > self._flush_batch_size:
                flush_event.set()

            log_created_event.clear()

    def _flush_worker(self, do_flush_event: InterruptableEvent):
        batch = []

        while True:
            do_flush_event.wait()

            if do_flush_event.is_interrupted:
                return

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
        self._item_enqueued_event = InterruptableEvent()
        self._flush_timeout_event = InterruptableEvent()
        self._flush_full_batch_event = InterruptableEvent()

        do_flush_event = any_event(
            self._flush_timeout_event,
            self._flush_full_batch_event
        )

        self._batch_worker_thread = Thread(
            target=self._batch_worker, 
            args=(self._item_enqueued_event, self._flush_full_batch_event,), 
            daemon=True
        )

        self._timed_worker_thread = Thread(
            target=self._timed_worker, 
            args=(self._flush_timeout_event, self),
            daemon=True
        )

        self._flush_worker_thread = Thread(
            target=self._flush_worker, 
            args=(do_flush_event,),
            daemon=True
        )

        self._timed_worker_thread.start()
        self._batch_worker_thread.start()
        self._flush_worker_thread.start()

        self.is_running = True

    def enqueue(self, item):
        if not self.is_running:
            return

        self._queue.put(item)
        self._item_enqueued_event.set()

    def stop(self):
        """
        Stops flush and bash worker threads immediately.
        The timed worker thread will stop after the next flush interval.
        """
        self.is_running = False
        self._item_enqueued_event.is_interrupted = True
        self._flush_timeout_event.is_interrupted = True
        self._flush_full_batch_event.is_interrupted = True

        # Set all events to force the worker threads to exit
        self._item_enqueued_event.set()
        self._flush_timeout_event.set()
        self._flush_full_batch_event.set()

        self._timed_worker_thread.join()
        self._batch_worker_thread.join()
        self._flush_worker_thread.join()
