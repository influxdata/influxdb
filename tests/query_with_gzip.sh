echo "querying data with gzip encoding"
curl -v -G --compressed http://localhost:8086/query --data-urlencode "db=foo" --data-urlencode "q=SELECT sum(value) FROM \"foo\".\"bar\".cpu GROUP BY time(1h)"
