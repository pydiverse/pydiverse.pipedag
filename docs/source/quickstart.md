# Quickstart

## Installation
pydiverse.pipedag is distributed on [PyPi](https://pypi.org/project/pydiverse-pipedag/).
To use it, just install it with pip:

```shell
pip install pydiverse-pipedag
```

## What is a Flow?
TODO...

- Flow consists of Stages
- Inside each stage there is a number of tasks.
  - Tasks get defined using the {py:func}`@materialize <pydiverse.pipedag.materialize>` decorator.


## Running your first Flow
In the following example we will create a very simple flow that 
loads to data frames into the database,
then uses SQL to join them,
and finally retrieves the joined table from the database as a data frame and prints it.

We'll start with defining the flow.
To do this, create a new python file (let's call it `quickstart.py`) inside an empty folder with the following content: 

```python
import pandas as pd
import sqlalchemy as sa

from pydiverse.pipedag import Flow, Stage, materialize

# Define the different tasks our flow consists of
@materialize(version="1.0", nout=2)
def input_tables():
    names = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["Alice", "Bob", "Charlie"],
    })

    ages = pd.DataFrame({
        "id": [1, 2, 3],
        "age": [20, 40, 60],
    })

    return names, ages

@materialize(lazy=True, input_type=sa.Table)
def join_tables(names, ages):
    return (
        sa.select(names.c.id, names.c.name, ages.c.age)
            .join_from(names, ages, names.c.id == ages.c.id)
    )

@materialize(input_type=pd.DataFrame)
def print_dataframe(df):
    print(df)

# Define how the different tasks should be wired
with Flow("flow") as flow:
    with Stage("inputs"):
        names, ages = input_tables()

    with Stage("features"):
        joined_table = join_tables(names, ages) 
        print_dataframe(joined_table)

# Execute the flow
flow.run()
```

Next, we need to tell how exactly this flow should get executed.
This is done using the [](/reference/config).
Create a new file called `pipedag.yaml` in the same folder as the python file and put the following text into it:
```yaml
instances:
  __any__:
    instance_id: quickstart
    auto_table: 
      - pandas.DataFrame
      - sqlalchemy.sql.expression.Selectable 
    
    table_store:
      class: "pydiverse.pipedag.backend.table.SQLTableStore"
      args:
        url: "postgresql://user:password@127.0.0.1:5432/{instance_id}"
        create_database_if_not_exists: true
    
    blob_store:
      class: "pydiverse.pipedag.backend.blob.FileBlobStore"
      args:
        base_path: "/tmp/pipedag/blobs"

    lock_manager:
      class: "pydiverse.pipedag.backend.lock.DatabaseLockManager"

    orchestration:
      class: "pydiverse.pipedag.engine.SequentialEngine"
```

Now you're almost ready to run the flow.
The only thing that's left to do is to start a database.
In this example we use a local postgres database.
You can spin one up on your computer using docker with the following command:
```shell
docker run --rm --name pipedag-quickstart -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_USER=user postgres
```

If you now run the flow by running `python quickstart.py`, you should see a bunch of logging output.
This is completely normal.
Somewhere within all the log lines you should see the following lines getting printed:
```none
   id     name  age
0   1    Alice   20
1   2      Bob   40
2   3  Charlie   60
```

This means that everything went as expected.

### What is going on here?

...