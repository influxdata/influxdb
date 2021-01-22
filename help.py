import requests

measurement = "test"

def write(index):
    requests.post(
        "http://localhost:9999/api/v2/write",
        params={"org": "myorg", "bucket": "test", "precision": "us"},
        headers={"Authorization": "Token sdfsdfsdfsdf"},
        data="""
        {},tag1=123456,tag2=dog field{}=0 1586378440176787
        {},tag1=123456,tag2=car field{}=1 1586378440195990
        """.format(measurement, index, measurement, index),
    )

def read(index):
    return requests.post(
        "http://localhost:9999/api/v2/query",
        params={"org": "myorg"},
        headers={
            "Authorization": "Token sdfsdfsdfsdf",
            "Accept": "application/csv",
            "Content-type": "application/json",
        },
        json={"dialect": {"header": False}, "query": """
        from(bucket: "test")
          |> range(start: 2020-04-06, stop: 2020-04-10)
          |> filter(fn: (r) => r._measurement == "{}")
          |> filter(fn: (r) => r._field == "field{}")
          |> keep(columns: ["_measurement", "_time", "_field", "_value", "tag1"])
          |> last()
        """.format(measurement, index)},
    )

nb_fails = 0
nb_total = 100
for i in range(nb_total):
    write(i)
    result = int(read(i).text.split(',')[4])
    print("test {}/{} -> {}".format(i, nb_total, result))
    if result != 1:
        nb_fails += 1

print("***********")
print("{} fails over {} tries".format(nb_fails, nb_total))