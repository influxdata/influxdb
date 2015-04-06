echo "creating database"
curl -G http://localhost:8086/query --data-urlencode "q=CREATE DATABASE foo"

echo "creating retention policy"
curl -G http://localhost:8086/query --data-urlencode "q=CREATE RETENTION POLICY bar ON foo DURATION 1h REPLICATION 3 DEFAULT"

echo '{"database" : "foo", "retentionPolicy" : "bar", "points": [{"name": "cpu", "tags": {"host": "server01"},"timestamp": "2015-01-26T22:01:11.703Z","fields": {"value": 123}}]}' | gzip > ./foo.zip

echo "inserting data"
curl -v -i --compressed -H "Content-encoding: gzip" -H "Content-Type: application/json" -X POST --data-binary ./foo.zip http://localhost:8086/write

rm ./foo.zip

echo "querying data with gzip encoding"
curl -v -G --compressed http://localhost:8086/query --data-urlencode "db=foo" --data-urlencode "q=SELECT sum(value) FROM \"foo\".\"bar\".cpu GROUP BY time(1h)"
