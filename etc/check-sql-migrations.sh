#!/bin/bash

# This script verifies that for all SQL migrations there is both an "up" and a "down", and that the file names match.

upMigrations=$(find sqlite/migrations/*.up.sql | cut -f 1 -d '.')
downMigrations=$(find sqlite/migrations/*.down.sql | cut -f 1 -d '.')

differences="$(diff -y --suppress-common-lines <(echo "$upMigrations" ) <(echo "$downMigrations"))"

if [[ -n ${differences} ]]
then
  echo '------------------------------------------------------------------------------------'
  echo "Problem detected with SQL migration files: Up and Down migration names do not match!"
  echo '------------------------------------------------------------------------------------'
  printf "%-36s%36s\n" "Up Migrations without Downs" "Down Migrations without Ups"
  echo "$differences"
  exit 1
fi
