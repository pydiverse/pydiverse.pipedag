# Pipedag Parallelize

This is a pytest plugin similar to `pytest-xdist` that allows executing tests in parallel.
To prevent two tasks that run on the same instance from corrupting each other's data,
it allows grouping tests together using the `pytest_parallelize_group_items` hook.
Tests that have been grouped, run sequentially on the same worker.
Different groups run in parallel on different workers.

To specify the number of workers, use the `--workers` argument.
