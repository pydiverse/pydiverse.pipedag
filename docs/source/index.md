---
hide-toc: true
---

# pydiverse.pipedag

Pydiverse pipedag is a data pipeline orchestration library used for organizing code in stages 
from data ingestion, over transformations, to model training and evaluation. Pipedag is meant 
for interoperability of tasks written with different data transformation 
languages that can serialize/deserialize tables to/from a relational database. 
Currently, we support tasks written with pandas, polars, tidypolars, sqlalchemy, ibis, and 
pydiverse transform code. While it is easy to integrate existing code, pipedag
removes boilerplate code for fast SQL table (de)materialization, caching and cache invalidation.

See [quickstart](/quickstart) for how to get started. A trivial pipedag pipeline could look like this: 
```python
# Define how the different tasks should be wired
with Flow() as flow:
    with Stage("inputs"):
        names, ages = input_tables()

    with Stage("features"):
        joined_table = join_tables(names, ages) 
        print_dataframe(joined_table)

# Execute the flow
flow.run()
```

Every task like `join_tables()` in the example above can choose the format in which it likes to 
access tables. In this example it uses SQLAlchemy to produce a JOIN-SQL-Query from two table references:
```python
@materialize(lazy=True, input_type=sa.Table)
def join_tables(names, ages):
    return (
        sa.select(names.c.id, names.c.name, ages.c.age)
            .join_from(names, ages, names.c.id == ages.c.id)
    )
```

The same could also be done with pandas:

```python
@materialize(version="1.0.0", input_type=pd.DataFrame)
def join_tables(names: pd.DataFrame, ages: pd.DataFrame):
    return names.merge(ages, on="id", how="inner")[["id", "name", "age"]]
```

Or with pydiverse transform:

```python
@materialize(lazy=True, input_type=pdt.SQLTableImpl)
def join_tables(names, ages):
    return names >> join(ages, names.id == ages.id) >> select(names.id, names.name, ages.age)
```

The main goal of pipedag is to support agility and iteration speed for 
teams developing data pipelines. It offers the following features:
1. easy embedding of existing data transformation, modeling, or model evaluation code 
    in a pipedag flow
2. value add out of the box: 

   - easy setup of multiple pipeline instances with small and big input data
   - materializing all tables in the database for easy inspection with explorative SQL queries
   - speedup by automatic caching and cache invalidation

3. incremental improvement of data pipeline code task by task, stage by stage
4. convenient unit- and integration-testing with many pipeline instances
5. stage transactionality concept ensures that a big input pipeline can be analyzed 
    with explorative SQL at the same time as the pipeline is updated
6. pipeline runs can be triggered from IDE debugger, continuous integration framework, or pipeline 
    orchestration UI without worrying about race conditions

Typical stages of a realistic pipeline are:
- raw ingestion
- cleaning for easier inspection (i.e. improve types for pandas.read_sql)
- transformation into best possible representation for economic reasoning
- feature engineering (both stateless and stateful)
- model training
- model evaluation

Here is how to try it out: [quickstart](/quickstart)

## pydiverse library collection

Pydiverse is a collection of libraries for describing data transformations 
and data processing pipelines.

Pydiverse pipedag is meant to wrap any kind of data processing pipeline code 
giving immediate benefits by making it easy to operate many pipeline instances 
with small and big input data and offering speedup by automatic caching and 
cache invalidation. A main goal is also to make it easy to improve data pipeline
code iteratively task by task, stage by stage.

Pydiverse transform is meant to write one syntax of data transformation code that
can be executed reliably on in-memory dataframes as well as SQL databases. The
interoperability of tasks in pipedag helps transform to limit scope and focus 
on quality. Results should be identical for different backends and good error messages
should be rased before sending a query to a backend when a particular feature is not 
supported, there.

There will be more focus on making unit- and integration-testing with many pipeline instances 
easy and convenient. This may justify another library called pydiverse pipetest.

With the aim to develop data pipeline code on small input data pipeline instances,
the generation of test data from the full input data may also be an area worth adding
a separate library called pydiverse testdata.

Check out pydiverse libraries on github:
- [pydiverse.pipedag](https://github.com/pydiverse/pydiverse.pipedag/)
- [pydiverse.transform](https://github.com/pydiverse/pydiverse.transform/)

Check out pydiverse libraries on readthedocs:
- [pydiverse.pipedag](https://pydiversepipedag.readthedocs.io/en/latest/)

[comment]: [pydiverse.transform](https://pydiversetransform.readthedocs.io/en/latest/)

## related work: standing on the sholders of giants

We strongly admire the clean library design achievements of [tidyverse](https://www.tidyverse.org/) 
and in particular [dplyr](https://dplyr.tidyverse.org/). We can recommend to read the documentation 
of dplyr to get a feel of how clean data wrangling might look like. However, we currently see the 
open source ecosystem in python win over the programming language R due to software engineering aspects. 
In general, we believe that the data science and software engineering tool stacks have to merge to get 
the best results in data analytics and machine learning.

Several popular tools like airflow - while being great pieces of work - go too far in taking care of 
execution so that interactive development within IDE debuggers is prohibitively complex.

[Pandas](https://pandas.pydata.org/) and [Polars](https://www.pola.rs/) are currently contending for what 
is the best dataframe processing library in python. We like them, use them, and support them. 
But they cannot tap into the genious ingenuity that went into developing relational databases which
are perfectly suited for supporting high iteration speed algorithm development in early exploration phases
as well as for disruptive improvement steps later on. Most structural data pools where flexible combining of
any data source with any other data source is key to success are **not** Big Data. Data pipelines 
do not need concurrent read and write operations on most tables but rather rely on the 
constant-after-construction principle for transformed tables. This simplifies scaling of relational database 
performance. It also results in data pipeline output solely depending on input data and the processing code. 
Thus, the emphasis of pipedag on caching and cache invalidation.

Leaving all intermediate calculation steps of a data pipeline lying around in the database is a nice property 
for analyzing problems and potential improvements with explorative SQL queries. However, handwritten SQL code
has a reputation for being hard to test and maintain. As a consequence, we want to make it easy to
transition incrementally from handwritten SQL or dataframe code to programmatically created SQL where better 
suited. [SQLAlchemy](https://www.sqlalchemy.org/) is one of the most basic and comprehensive tools to do so. 
However, convenience is not that great. [IBIS](https://ibis-project.org/) set out to offer a dplyr like user 
interface for generating SQL. It also tries to support dataframe backends, but we are not satisfied with the 
current quality. It will also be hard to improve quality significantly across the vast number of supported 
backends and ambitions to support all SQL capabilities. [Siuba](https://siuba.org/) was actually the 
startingpoint for pydiverse transform. But it also was not at a satisfying maturity level. 
On the dataframe side, there is also a dplyr like user interface: 
[tidypolars](https://github.com/markfairbanks/tidypolars). It looks promising, but it did not keep always up 
with the newest polars version. We would love to use IBIS and tidypolars in whatever area pydiverse 
transform is not yet ready to do the job or where we itentionally restrict scope to ensure quality.

There are translation tools going from one data transformation language to multiple target backends 
that are far more ambitious than pydiverse: [substrait](https://substrait.io/) 
and [data_algebra](https://github.com/WinVector/data_algebra).
We are watching developments with excitement, but are more convinced that we can ensure reliable operation
for a defined set of backends, types, and scope with pydiverse transform. However, all of those techniques are 
candidates for pydiverse pipedag integration in case of user demand.

The flow declaration syntax of pipedag was inspired by prefect 1.x. They moved to a new syntax, but we still 
support [prefect](https://docs.prefect.io/latest/) as an execution engine and pipeline orchestration UI. They 
changed from separate flow declaration to an async await model. Despite requiring the user to understand the 
difference between flow declaration time and task execution time, we prefer the explicit delaration model in 
pipedag since it allows us to perform actions on the graph before executing anything. For example, we can tap 
into intermediate tables with interactive code or run just individual tasks or stages. We also think the 
structure of data pipelines is rather static in nature. And it helps to 
guarantee it is that way without having to look into implementation details.

When using handwritten SQL, [dbt](https://www.getdbt.com/) is a mature tool sharing some concepts with 
pydiverse pipedag. It also lets the user just provide a `SELECT` query and takes care of materializing the 
transformed data with a `CREATE TABLE AS SELECT` query. Therefore, both tools can configure the location of 
data seperately from the transformation code. Both tools can detect whether a query needs to be reexecuted 
depending on input data and query changes (automatic cache invalidation). However, despite dbt claiming 
to support python, the latter is not in the driver seat but python processes are just started on demand 
to execute certain tasks. 
This makes it hard to reliably prepare state-of-the-art python environments 
 and to embed them in the cache invalidation process. 
This complicates the management of multiple pipeline instances composed of both SQL parts and python modeling 
code. It also is an impediment towards gradually moving to programmatically created SQL. 
Instead, most dbt users rely on jinja templated SQL strings. Dbt does not support the concept of multiple
pipeline instances natively.


[//]: # (Contents of the Sidebar)

```{toctree}
:hidden:

quickstart
reference/config
reference/api
reference/cli
```

```{toctree}
:caption: Development
:hidden:

changelog
license