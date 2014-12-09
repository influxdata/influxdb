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

    LIST SERIES

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
