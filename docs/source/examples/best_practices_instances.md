# Best practices: multiple instances: full_fresh, full_stable, mini_stable, midi_stable

This story expands on the [multi_instance_pipeline example](multi_instance_pipeline) storyline.

In general, data pipelines are processing a considerable amount of information be it tables with 100k to 100 million rows
or even billions. Thus processing times will be many minutes or hours. However, iteration speed of software development
on the pipeline is key because the pipeline is used to transform the data in a way that increases understanding and from
better understanding come changes to the code in the data pipeline.

As a consequence, you should not just have one data pipeline. You should always have at least two little siblings for any
pipeline:
* mini: The minimal amount of data that allows the pipeline code to run through technically.
* midi: A somewhat reasonable selection of data which reaches a high level of code coverage, triggers most edge cases the
pipeline code is concerned with, and may be sampled in a way that allows for statistically sound conclusions be it with
reduced statistical prediction power or higher error margins. If all goals cannot be met with one subset of the input
data, more pipeline instances may be needed.

Another concern to worry about is that for some purposes, fresh data is required, however, for understanding data and
developing statistically significant models, it is actually rather harmful to have changing data and changing code at the
same time. If you train your model on 1-3 years worth of data, then adding the latest days or weeks does not really provide
much value. Thus it may even be beneficial to have separate pipelines working on fresh data and on stable data.

The prototypical setup of a pipeline instances with different sizes and different freshness is:
- full fresh pipeline (sources raw input layer)
- full stable pipeline (feeds from full fresh raw input layer)
- midi stable pipeline (feeds from full stable raw input layer and filters)
- mini stable pipeline (feeds from full stable raw input layer and filters)

Filtering between pipeline instances is nice because like this, it is guaranteed that all stable pipelines are in-sync
capturing the same data version. And it is nice because generic filtering technology can be developed independent of
where data is sourced from. In the future this code could also be provided by a separate pydiverse library.

For development and test of the source loading technology, it might also be nice to keep an additional instance which,
however, should not be used for developing the actual pipeline:
- mini fresh: performs the same loading technology than the full fresh pipeline, but only loads a minimal amount of data.

For the full stable pipeline, it is important that the data is not changing. This can be achieved by switching the
option `cache_validation: mode:` to "assert_no_fresh_input" for this pipeline once the data is loaded and should be
kept stable. On the one hand, this implies ignoring cache functions mentioned in @materialize() decorator and additionally,
it fails in case a task changes that has a cache function and thus might bring in data from external sources.

An example showing how to implement this can be found here: [](/examples/multi_instance_pipeline).