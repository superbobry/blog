Title:
Date:
Category: programming
Tags: python, concurrency

I came around a curious edge-case of the `concurrent.futures` module introduced
in PEP-3148. Here's a gist

```python
with ThreadPoolExecutor() as executor:
    futures = []
    for task in tasks:
        futures.append(executor.submit(task))

    # Re-raise the exception.
    for future in as_completed(futures):
        future.result()
```

The problem: if one of the tasks fail, the exception is reraised by `result`,
which gets the main thread inside `ThreadPoolExecutor.__exit__`. `__exit__`
blockingly waits for all tasks to terminate. Therefore, the exception is
not printed until all tasks finish processing.

Solution 0: cancel the futures.

Doesn't work because thread-backed future can only be canceled until it
has been scheduled on a thread. Fucking Python.

Solution 1: pool threads are daemon; for our problem, if one of the tasks
fails, all should fail, therefore we could just exit the interpreter w/o
finilizing the executor.

```
futures = []
for task in tasks:
    futures.append(executor.submit(task))

# Re-raise the exception.
for future in as_completed(futures):
    future.result()
```

doesn't work because there is an exit hook for thread pool, which ensures
that each worker thread finishes whatever they are running.

Hacky solution:

```
        try:
            for future in as_completed(futures):
                future.result()
        except Exception:
            import atexit
            from concurrent.futures.thread import _python_exit
            atexit.unregister(_python_exit)
            raise
        else:
            executor.shutdown()
```
