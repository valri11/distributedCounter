#!/bin/bash
# This script is run once by the postgres container if the database does not exist yet

set -e

# We have three users (the root user and in addition we create two new users):
# - A root user which should only be used to create the initial database and provision users 
#   and privileges.
# - A migrate user to run migrations on the database. This user has all privileges on the database
#   and for real DB instances will be used from CI or local in order to run migrations. It will
#   therefore also be the owner of the tables.
# - An app user to be used by the application. This user only has the permissions needed for 
#   normal service operation. 
#
# We need to alter the default privileges for the migrate user in order to assign privileges to
# future tables to the app user on the newly created database.

PSQL_CMD="psql -v ON_ERROR_STOP=1"

# Add host to the command if DB_HOST is set
if [ -n "$DB_HOST" ]; then
    PSQL_CMD="$PSQL_CMD -h \"$DB_HOST\""
fi

# Add the username and dbname to the command
PSQL_CMD="$PSQL_CMD --username \"$POSTGRES_USER\" --dbname \"$POSTGRES_DB\""

# Execute the command with the SQL script
eval "$PSQL_CMD" <<-EOSQL
    CREATE DATABASE $DB_DATABASE;
EOSQL
