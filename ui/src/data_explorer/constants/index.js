export const INFLUXQL_FUNCTIONS = [
  'mean',
  'median',
  'count',
  'min',
  'max',
  'sum',
  'first',
  'last',
  'spread',
  'stddev',
]

export const QUERY_TEMPLATES = [
  {text: 'Show Databases', query: 'SHOW DATABASES'},
  {text: 'Create Database', query: 'CREATE DATABASE "db_name"'},
  {text: 'Drop Database', query: 'DROP DATABASE "db_name"'},
  {text: 'Show Measurements', query: 'SHOW MEASUREMENTS ON "db_name"'},
  {
    text: 'Show Tag Keys',
    query: 'SHOW TAG KEYS ON "db_name" FROM "measurement_name"',
  },
  {
    text: 'Show Tag Values',
    query: 'SHOW TAG VALUES ON "db_name" FROM "measurement_name" WITH KEY = "tag_key"',
  },
  {
    text: 'Show Retention Policies',
    query: 'SHOW RETENTION POLICIES on "db_name"',
  },
  {
    text: 'Create Retention Policy',
    query: 'CREATE RETENTION POLICY "rp_name" ON "db_name" DURATION 30d REPLICATION 1 DEFAULT',
  },
  {
    text: 'Drop Retention Policy',
    query: 'DROP RETENTION POLICY "rp_name" ON "db_name"',
  },
  {
    text: 'Create Continuous Query',
    query: 'CREATE CONTINUOUS QUERY "cq_name" ON "db_name" BEGIN SELECT min("field") INTO "target_measurement" FROM "current_measurement" GROUP BY time(30m) END',
  },
  {
    text: 'Drop Continuous Query',
    query: 'DROP CONTINUOUS QUERY "cq_name" ON "db_name"',
  },
  {text: 'Show Users', query: 'SHOW USERS'},
  {
    text: 'Create User',
    query: 'CREATE USER "username" WITH PASSWORD \'password\'',
  },
  {
    text: 'Create Admin User',
    query: 'CREATE USER "username" WITH PASSWORD \'password\' WITH ALL PRIVILEGES',
  },
  {text: 'Drop User', query: 'DROP USER "username"'},
  {text: 'Show Stats', query: 'SHOW STATS'},
  {text: 'Show Diagnostics', query: 'SHOW DIAGNOSTICS'},
]
