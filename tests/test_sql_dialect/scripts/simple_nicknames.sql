BEGIN
 IF EXISTS (SELECT * FROM SYSCAT.WRAPPERS WHERE WRAPNAME = 'DRDA')
  THEN EXECUTE IMMEDIATE 'DROP WRAPPER DRDA';
 END IF;
END|
CREATE WRAPPER DRDA|
CREATE SERVER remote_db TYPE DB2/LUW VERSION 11 WRAPPER DRDA
    AUTHORIZATION "db2inst1" PASSWORD "password" OPTIONS (
  HOST '127.0.0.1', PORT '50000', DBNAME 'testdb'
  )|

CREATE NICKNAME {{out_schema}}.nick1 FOR remote_db.{{out_schema}}.{{out_table}}|
CREATE NICKNAME {{out_schema}}.nick2 FOR remote_db.{{out_schema}}.{{out_table}}|