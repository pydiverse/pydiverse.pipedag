import prefect

def schema_ref_counter_handler(task, old_state, new_state):
    """Prefect Task state handler to update the schema reference counter"""

    if isinstance(new_state, prefect.engine.state.Mapped):
        # Is mapping task -> Increment reference counter by the number
        # of child tasks.
        task._incr_schema_ref_count(new_state.n_map_states)

    if isinstance(new_state, prefect.engine.state.Failed):
        run_count = prefect.context.get('task_run_count', 0)
        if run_count <= task.max_retries:
            # Will retry -> Don't decrement ref counter
            return

    if isinstance(new_state, prefect.engine.state.Finished):
        # Did finish task and won't retry -> Decrement ref counter
        task._decr_schema_ref_count()
