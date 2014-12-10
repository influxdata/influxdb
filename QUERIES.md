The top level name is called a measurement. These names can contain any characters. Then there are field names, field values, tag keys and tag values, which can also contain any characters. Because of this, anywhere a measurement name, field name, field value, tag name, or tag value appears should be able to get wrapped in double quotes to deal with special characters.

# Select

```sql
-- get the top 10 host measurements for the last hour (same host can appear multiple times)
SELECT top(10, value), host FROM cpu WHERE time > now() - 1h

-- get the top 10 unique hosts for the last hour
SELECT top(10, value), distinct(host) FROM cpu WHERE time > now() - 1h
```

## Group By

# Delete

# Series

## Destroy

```sql
DROP MEASUREMENT <name>
DROP MEASUREMENT cpu WHERE region = 'uswest'
```

## List

List series queries are for pulling out individual series from measurement names and tag data. They're useful for discovery.

```sql
-- list measurement names
LIST MEASUREMENTS
LIST MEASUREMENTS WHERE service = 'redis'

-- list all series across all measurements/tagsets
LIST SERIES

-- get a list of all series for any measurements where tag key region = tak value 'uswest'
LIST SERIES WHERE region = 'uswest'

-- get a list of all tag keys across all measurements
LIST TAG KEYS

-- list all the tag keys for a given measurement
LIST TAG KEYS FROM cpu
LIST TAG KEYS FROM temperature, wind_speed

-- list all the tag values. note that at least one WHERE key = '...' clause is required
LIST TAG VALUES WHERE key = 'region'
LIST TAG VALUES FROM cpu WHERE region = 'uswest' and key = 'host'

-- and you can do stuff against fields
LIST FIELD KEYS FROM cpu

-- but you can't do this
LIST FIELD VALUES
-- we don't index field values, so this query should be invalid.
```

Note that `FROM` and `WHERE` are optional clauses in all of the list series queries.

And the list series output looks like this:

```json
[
    {
        "name": "cpu",
        "columns": ["id", "region", "host"],
        "values": [
            1, "uswest", "servera",
            2, "uswest", "serverb"
        ]
    },
    {
        "name": "reponse_time",
        "columns": ["id", "application", "host"],
        "values": [
            3, "myRailsApp", "servera"
        ]
    }
]
```

# Continuous Queries

Continous queries are going to be inspired by MySQL `TRIGGER` syntax:

http://dev.mysql.com/doc/refman/5.0/en/trigger-syntax.html

Instead of having automatically-assigned ids, named continuous queries allows for some level of duplication prevention,
particularly in the case where creation is scripted.

## Create

    CREATE CONTINUOUS QUERY <name> AS SELECT ... FROM ...

## Destroy

    DROP CONTINUOUS QUERY <name>

## List

    LIST CONTINUOUS QUERIES
