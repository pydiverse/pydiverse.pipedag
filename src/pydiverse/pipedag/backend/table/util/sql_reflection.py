from __future__ import annotations

import sqlalchemy as sa


class PipedagDB2Reflection:
    @staticmethod
    def get_alias_names(engine: sa.Engine, schema: str) -> list[str]:
        """Returns all aliases in a schema"""

        schema = engine.dialect.denormalize_name(schema)

        query = f"""
        SELECT TABNAME
        FROM SYSCAT.TABLES
        WHERE TABSCHEMA = '{schema}' AND TYPE = 'A'
        """

        with engine.connect() as conn:
            aliases = conn.exec_driver_sql(query).scalars().all()

        aliases = [engine.dialect.normalize_name(name) for name in aliases]
        return list(aliases)

    @staticmethod
    def resolve_alias(engine: sa.Engine, name: str, schema: str) -> tuple[str, str]:
        """Recursively resolves an alias

        :returns: A tuple (table_name, schema)
        """

        _schema = engine.dialect.denormalize_name(schema)
        _name = engine.dialect.denormalize_name(name)

        # Recursive CTE query to resolve alias
        query = f"""
        WITH aliases (TABSCHEMA, TABNAME, BASE_TABSCHEMA, BASE_TABNAME, LEVEL) as
            (SELECT a.TABSCHEMA, a.TABNAME, a.BASE_TABSCHEMA, a.BASE_TABNAME, 1
                    FROM SYSCAT.TABLES a
                    WHERE TABSCHEMA = '{_schema}'
                      AND TABNAME = '{_name}'
                      AND TYPE = 'A'
             UNION ALL
             SELECT b.TABSCHEMA, b.TABNAME, b.BASE_TABSCHEMA, b.BASE_TABNAME, r.LEVEL+1
                    FROM aliases r, SYSCAT.TABLES b
                    WHERE r.BASE_TABSCHEMA = b.TABSCHEMA
                      AND r.BASE_TABNAME = b.TABNAME
                      AND b.TYPE = 'A'
                      AND r.LEVEL < 100)
        SELECT BASE_TABNAME, BASE_TABSCHEMA FROM aliases
        ORDER BY LEVEL DESC
        LIMIT 1
        """

        with engine.connect() as conn:
            if result := conn.exec_driver_sql(query).one_or_none():
                return (
                    engine.dialect.normalize_name(result[0]),
                    engine.dialect.normalize_name(result[1]),
                )
        return name, schema


class PipedagMSSqlReflection:
    @staticmethod
    def get_alias_names(engine: sa.Engine, schema: str):
        query = f"""
        SELECT syn.name
        FROM sys.synonyms AS syn
        LEFT JOIN sys.schemas AS schem
               ON syn.schema_id = schem.schema_id
        WHERE schem.name = '{schema}'
        """

        with engine.connect() as conn:
            result = conn.exec_driver_sql(query).scalars().all()
        return result

    @staticmethod
    def resolve_alias(
        engine: sa.Engine, name: str, schema: str
    ) -> tuple[str, str] | tuple[None, None]:
        from sqlalchemy.dialects.mssql.base import _schema_elements

        query = f"""
        SELECT syn.base_object_name
        FROM sys.synonyms AS syn
        LEFT JOIN sys.schemas AS schem
               ON syn.schema_id = schem.schema_id
        WHERE schem.name = '{schema}'
          AND syn.name = '{name}'
          AND syn.type = 'SN'
        """

        with engine.connect() as conn:
            base_object_name = conn.exec_driver_sql(query).scalar_one_or_none()

        if base_object_name:
            owner, table = _schema_elements(base_object_name)
            return table, owner

        return name, schema

    @staticmethod
    def get_procedure_names(engine: sa.Engine, schema: str):
        query = f"""
        SELECT obj.name
        FROM sys.objects AS obj
        LEFT JOIN sys.schemas AS schem
               ON obj.schema_id = schem.schema_id
        WHERE schem.name = '{schema}'
          AND obj.type = 'P'
        """

        with engine.connect() as conn:
            result = conn.exec_driver_sql(query).scalars().all()
        return result

    @staticmethod
    def get_function_names(engine: sa.Engine, schema: str):
        query = f"""
        SELECT obj.name
        FROM sys.objects AS obj
        LEFT JOIN sys.schemas AS schem
               ON obj.schema_id = schem.schema_id
        WHERE schem.name = '{schema}'
          AND obj.type = 'FN'
        """

        with engine.connect() as conn:
            result = conn.exec_driver_sql(query).scalars().all()
        return result
