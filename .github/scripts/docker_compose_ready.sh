#!/bin/bash
# This script checks if all the services defined in our docker compose file
# are up and running.

set -e
set -o pipefail

POSTGRES=0
MSSQL=0
IBM_DB2=0
MINIO=0
PREFECT=0
ZOO=0

while test x"$1" != x; do
  case "$1" in
    postgres)
      POSTGRES=1
      ;;
    mssql)
      MSSQL=1
      ;;
    ibm_db2)
      IBM_DB2=1
      ;;
    minio)
      MINIO=1
      ;;
    prefect)
      PREFECT=1
      ;;
    zoo)
      ZOO=1
      ;;
  esac;
  shift;
done

running_services=$(docker compose ps --services --status running)

echo "Running services: $(echo $running_services | tr '\n' ' ')"

if [[ "$POSTGRES" != 0 ]]; then
	if ! [[ "$running_services" =~ "postgres" ]] || ! docker compose logs postgres 2>&1 | grep "database system is ready to accept connections" > /dev/null; then
    echo "PostgreSQL is not ready yet."
    exit 1
  fi
fi

if [[ "$MSSQL" != 0 ]]; then
	if ! [[ "$running_services" =~ "mssql" ]] || ! docker compose logs mssql 2>&1 | grep "SQL Server is now ready for client connections" > /dev/null; then
    echo "MSSQL is not ready yet."
    exit 1
  fi
fi

if [[ "$IBM_DB2" != 0 ]]; then
	if ! [[ "$running_services" =~ "ibm_db2" ]] || ! docker compose logs ibm_db2 2>&1 | grep "Setup has completed" > /dev/null; then
    echo "IBM DB2 is not ready yet."
    exit 1
  fi
fi

if [[ "$MINIO" != 0 ]]; then
	if ! [[ "$running_services" =~ "minio" ]] || ! docker compose logs minio 2>&1 | grep "Docs: https://docs.min.io" > /dev/null; then
    echo "MINIO is not ready yet."
    exit 1
  fi
fi

if [[ "$PREFECT" != 0 ]]; then
	if ! [[ "$running_services" =~ "prefect" ]] || ! docker compose logs prefect 2>&1 | grep "Check out the dashboard at" > /dev/null; then
    echo "PREFECT is not ready yet."
    exit 1
  fi
fi

if [[ "$ZOO" != 0 ]]; then
	if ! [[ "$running_services" =~ "zoo" ]] || ! echo ruok | nc localhost 2181 > /dev/null; then
    echo "Zookeeper is not ready yet."
    exit 1
  fi
fi

exit 0
