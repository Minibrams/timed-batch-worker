# Timed Batch Worker

Python library for efficiently batch-processing workloads asynchronously with batch-size- and time-based flushes using short-circuiting events.

## Installation
```bash
pip install timed-batch-worker
```

## Usage
```python
from timed_batch_worker import TimedBatchWorker


# Handle the batched items
def handle_batch(items):
  http.post('/api/logs/bulk', data=items)


# Create a worker that flushes the queue when:
# 1. The batch contains 20 objects, or
# 2. Five seconds have passed since the last flush
worker = TimedBatchWorker(
  handler=handle_batch,
  flush_interval=5,
  flush_batch_size=20,
)

# Worker started in separate thread to avoid throttling main event loop
# in case the batch handler includes synchronous workloads.
worker.start()


# Add objects to the current batch at any time
@app.get('/users/create')
def create_user():
  ...

  # Hand off an object to the worker
  worker.enqueue({
    'event': 'USER_CREATED',
    'data': user
  })

  ...
```



