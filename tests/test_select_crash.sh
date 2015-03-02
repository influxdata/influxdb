echo "creating database"
curl -G http://localhost:8086/query --data-urlencode "q=CREATE DATABASE test_crash"
echo
echo "creating retention policy"
curl -G http://localhost:8086/query --data-urlencode "q=CREATE RETENTION POLICY bar ON test_crash DURATION 1h REPLICATION 1 DEFAULT"
echo
echo "inserting data"
curl -d '{"database" : "test_crash", "points" : [{"fields" : {"value" : "1424299535077"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299535077}, {"fields" : {"value" : "1424299541765"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299541765}, {"fields" : {"value" : "1424299548516"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299548516}, {"fields" : {"value" : "1424299555014"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299555014}, {"fields" : {"value" : "1424299561743"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299561743}, {"fields" : {"value" : "1424299568564"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299568564}, {"fields" : {"value" : "1424299575370"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299575370}, {"fields" : {"value" : "1424299582198"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299582198}, {"fields" : {"value" : "1424299586500"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299586500}, {"fields" : {"value" : "1424299591893"}, "name" : "power", "precision" : "ms", "timestamp" : 1424299591893}], "retentionPolicy" : "bar", "tags" : {"location" : "eg", "source" : "s1", "type" : "consume"}}' -H "Content-Type: application/json" http://localhost:8086/write
echo
echo "querying data regular select"
curl -G http://localhost:8086/query --data-urlencode "db=test_crash" --data-urlencode "q=SELECT value FROM power WHERE type = 'consume' AND location='eg' AND source='s1'"
echo
echo "querying data select mean --> provoke crash"
curl -G http://localhost:8086/query --data-urlencode "db=test_crash" --data-urlencode "q=SELECT mean(value) FROM power WHERE type = 'consume' AND location='eg' AND source='s1'"
echo
