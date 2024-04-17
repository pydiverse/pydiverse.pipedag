# Best Practices for data pipelines

The Python community is very concerned with enabling users to stitch together a few code snippets that run as a py file 
or jupyter notebook. However, in practice, projects trying to extract significant business impact from data analytics
very quickly reach a size where more sophisticated code organization is needed. On the one hand, this relates to software 
engineering principles like modularization, unit/integration testing, IDE support, CI/CD. On the other hand, data processing
steps are best organized as a pipeline or graph of steps/tasks. Those data pipelines are the focus of the following
best practice suggestions:

* [moving from Raw SQL over handwritten SELECT statements to programmatic SQL](/examples/best_practices_sql)
* [multiple instances: full_fresh, full_stable, mini_stable, midi_stable](/examples/best_practices_instances)
* [inline views, CTEs, and subqueries](/examples/best_practices_inline)

```{toctree}
/examples/best_practices_sql
/examples/best_practices_instances
/examples/best_practices_inline
```