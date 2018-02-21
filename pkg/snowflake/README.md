Snowflake ID generator
======================

This is a Go implementation of [Twitter Snowflake](https://blog.twitter.com/2010/announcing-snowflake).

The most useful aspect of these IDs is they are _roughly_ sortable and when generated
at roughly the same time, should have values in close proximity to each other.

IDs
---

Each id will be a 64-bit number represented, structured as follows:


```
6  6         5         4         3         2         1
3210987654321098765432109876543210987654321098765432109876543210

ttttttttttttttttttttttttttttttttttttttttttmmmmmmmmmmssssssssssss
```

where

* s (sequence) is a 12-bit integer that increments if called multiple times for the same millisecond
* m (machine id) is a 10-bit integer representing the server id
* t (time) is a 42-bit integer representing the current timestamp in milliseconds
  the number of milliseconds to have elapsed since 1491696000000 or 2017-04-09T00:00:00Z

### String Encoding

The 64-bit unsigned integer is base-63 encoded using the following URL-safe characters, which are ordered
according to their ASCII value.

```
0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz~
```

A binary sort of a list of encoded values will be correctly ordered according to the numerical representation. 