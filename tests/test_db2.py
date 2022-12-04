import ibm_db
import pandas as pd
import sqlalchemy as sa
import structlog


def test_db2():
    logger = structlog.getLogger(module=__name__)
    conn = ibm_db.connect(
        "DATABASE=testdb;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP;UID=db2inst1;PWD=password;",
        "",
        "",
    )
    query = "SELECT 1 as a FROM SYSIBM.SYSDUMMY1"
    stmt = ibm_db.exec_immediate(conn, query)
    row = True
    rows = []
    while row is not False:
        row = ibm_db.fetch_assoc(stmt)
        print(row)
        logger.info("DB2 test row", row=row)
        rows.append(row)


def test_db2_sqlalchemy():
    logger = structlog.getLogger(module=__name__)
    engine = sa.create_engine("db2+ibm_db://db2inst1:password@localhost:50000/testdb")
    df = pd.read_sql("SELECT 1 as a FROM SYSIBM.SYSDUMMY1", con=engine)
    logger.info("DB2 test df", df="\n" + str(df))
