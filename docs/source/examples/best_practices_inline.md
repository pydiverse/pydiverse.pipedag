# Best practices: inline views, CTEs, and subqueries

The big advantage of relational databases is that we can combine every table with every other table in 
a wide range of ways and don't need to worry about how exactly this should be executed. Especially, we
don't need to estimate resource consumption of various implementation alternatives in order to avoid out
of memory errors and alike. 

Performance wise, however, it is important to understand a bit what the database is doing under the hood.
The main task they are solving in the 1-100 million row range is to avoid O(n^2) complexity when joining 
two tables since in theory, a join could combine very row of one table with every row of the other table 
and only understanding the `ON` and `WHERE` conditions as well as indexes allows to prevent this quadratic 
explosion.

Query optimizers of relational databases are decades matured, highly sophisticated technology, which can
analyze a vast search space of options. However, at some point they need to stop planning and start executing
the query and thus we need to reduce the risk that this happens before the optimizer understood the main joins
between large tables in the query. There is a somewhat unpopular, but highly effective procedure to massively
reduce the risk of rogue queries where the query optimizer resorted to a brute force method saturating at least
one resource of your database and thus also reducing the performance of other users of the same database:

**Stop using CTEs, subqueries, and views!**

They may be a means of raw SQL code organization. And to some extent they work fine as intendent. And then comes
the flap of a butterfly and a query suddenly incurs a slowdown of 10-1000x for no reason. Especially, in the 
1-100 million row range, this plays out most significant. A slowdown from a 5 min query to 3 hours is not uncommon.
The good news: [programmatic SQL](/examples/best_practices_sql) can be used as an alternative means of SQL code 
organization where the finally assembled query does not contain any CTEs, subqueries, or views any more.

Pydiverse.pipedag can also hide the existence of multiple table materializations within a function that looks like
a task:

```python
import sqlalchemy as sa
import pydiverse.pipedag as dag
from pydiverse.pipedag import materialize

def table01(raw01: sa.Alias):
    @materialize(lazy=True, input_type=sa.Table)
    def raw01x(raw01: sa.Alias):
        return dag.Table(sa.select([raw01.c.entity]).distinct(), name="raw01x")

    @materialize(lazy=True, input_type=sa.Table)
    def query(raw01: sa.Alias, raw01x: sa.Alias):
        sql = (
            sa.select([raw01.c.entity, sa.literal("Missing in raw01").label("reason")])
            .select_from(raw01.outer_join(raw01x, raw01.c.entity == raw01x.c.entity))
            .where((raw01.c.end_date == "9999-01-01") & (raw01x.c.entity.is_(None)))
        )
        return dag.Table(sql, name="table01", primary_key=["entity", "reason"])

    return query(raw01, raw01x(raw01))
```
Attention: sa.Alias only exists for SQLAlchemy >= 2.0. Use sa.Table or sa.sql.expression.Alias for older versions.

Another alternative is to use [imperative materialization](/examples/imperative_materialization):
```python
import sqlalchemy as sa
import pydiverse.pipedag as dag
from pydiverse.pipedag import materialize

@materialize(lazy=True, input_type=sa.Table)
def table01(raw01: sa.Alias):
    raw01x_sql = sa.select([raw01.c.entity]).distinct()
    raw01x = dag.Table(raw01x_sql, name="raw01x").materialize()

    sql = (
        sa.select([raw01.c.entity, sa.literal("Missing in raw01").label("reason")])
        .select_from(raw01.outer_join(raw01x, raw01.c.entity == raw01x.c.entity))
        .where((raw01.c.end_date == "9999-01-01") & (raw01x.c.entity.is_(None)))
    )
    return dag.Table(sql, name="table01", primary_key=["entity", "reason"])
```