# Select

## Having

```sql
SELECT COUNT(value) FROM some_series GROUP BY TIME(5m) HAVING COUNT(value) > 23

SELECT top(10, value, host),  host FROM cpu WHERE time > now() - 1h

SELECT MAX(value) AS max_value, host FROM cpu GROUP BY TIME(1h), host HAVING TOP(max_value, 13)
```

## Group By

# Delete

# Series

## Destroy

    DROP SERIES <name>

## List

List series queries are for pulling out individual series from measurement names and tag data. They're useful for discovery.

```sql
-- list all series across all measurements/tagsets
LIST SERIES

-- get a list of all series for any measurements where tag key region = tak value 'uswest'
LIST SERIES WHERE region = 'uswest'

-- get a list of all tag keys across all measurements
LIST KEYS

-- list all the tag keys for a given measurement
LIST KEYS WHERE measurement = 'cpu'
LIST KEYS WHERE measurement = 'temperature' or measurement = 'wind_speed'

-- list all the tag values. note that at least one WHERE key = '...' clause is required
LIST VALUES WHERE key = 'region'
LIST VALUES WHERE measurement = 'cpu' and region = 'uswest' and key = 'host'

-- or maybe like this?
LIST VALUES("host") WHERE measurement = 'cpu'
-- or?
LIST host WHERE measurement = 'cpu'
-- or just use the select syntax
SELECT DISTINCT("host") from cpu
-- but then it would be tricky to show all values of host across 
-- all measurements, which we need to be able to do
```

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
