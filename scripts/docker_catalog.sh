#!/bin/bash
set -eu -o pipefail

# bail out if it's already running; let the user delete it if that's what they want
docker container inspect postgres &> /dev/null && \
  { echo "Postgres container already running. Remove it with \`docker container rm -f postgres\` and run again."; exit 1; }

# start postgres docker container
echo "Starting Postres container..."
docker container run -d \
  --name postgres \
  -e POSTGRES_HOST_AUTH_METHOD=trust \
  -p 5432:5432 \
  postgres:latest > /dev/null # redirecting only stdout so user can see errors such as port conflicts

# wait for it to come up
SECONDS=0
until [[ $(docker container inspect postgres | jq -r .[0].State.Status) == "running" ]]
do
  if (( SECONDS > 15 ))
  then
    echo "Postgres container hasn't started yet. Giving up"
    exit 1
  fi
  sleep 0.25
done
echo "Postgres is up. Running migrations..."

# run migrations
export INFLUXDB_IOX_CATALOG_DSN="postgresql://postgres@localhost:5432/postgres"
export DATABASE_URL="${INFLUXDB_IOX_CATALOG_DSN}"
cargo sqlx database create
cargo run -q -- catalog setup

echo "Enjoy your database! Point IOx to it by running the following:"
echo "\$ export INFLUXDB_IOX_CATALOG_DSN=\"${DATABASE_URL}\""
