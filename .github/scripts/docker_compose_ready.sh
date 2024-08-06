#!/bin/bash
# This script checks if all the services defined in our docker compose file
# are up and running.

set -e 
set -o pipefail

running_services=$(docker compose ps --services --status running)

if [[ "$running_services" =~ "postgres" ]]; then
	docker compose logs postgres 2>&1 | grep "database system is ready to accept connections" > /dev/null
fi

if [[ "$running_services" =~ "mssql" ]]; then
	docker compose logs mssql 2>&1 | grep "SQL Server is now ready for client connections" > /dev/null
fi

if [[ "$running_services" =~ "ibm_db2" ]]; then
	docker compose logs ibm_db2 2>&1 | grep "Setup has completed" > /dev/null
fi

if [[ "$running_services" =~ "zoo" ]]; then
	echo ruok | nc localhost 2181 > /dev/null
fi
