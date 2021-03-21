For more detailed and up to date information check out the [GitHub Repository](https://github.com/influxdata/influxdb-client-swift/)

##### Install via Swift Package Manager

Add this line to your `Package.swift`:

```swift
// swift-tools-version:5.3
import PackageDescription

let package = Package(
    name: "MyPackage",
    dependencies: [
        .package(name: "influxdb-client-swift", url: "https://github.com/influxdata/influxdb-client-swift", from: "0.2.0"),
    ],
    targets: [
        .target(name: "MyModule", dependencies: [
          .product(name: "InfluxDBSwift", package: "influxdb-client-swift"),
          // or InfluxDBSwiftApis for management API
          .product(name: "InfluxDBSwiftApis", package: "influxdb-client-swift")
        ])
    ]
)
```

##### Creating a client

```swift
import Foundation
import InfluxDBSwift

let url = "<%= server %>"
let token = "<%= token %>"
let bucket = "<%= bucket %>"
let org = "<%= org %>"

let client = InfluxDBClient(url: url, token: token)

// always close client at the end
client.close()
```

##### Write Data

```swift
//
// Record defined as String
//
let recordString = "demo,type=string value=1i"

client.makeWriteAPI().write(record: recordString) { result, error in
    // For handle error
    if let error = error {
        print("Error:\n\n\(error)")
    }

    // For Success write
    if result != nil {
        print("Successfully written data:\n\n\(recordString)")
    }
}

//
// Record defined as Data Point
//
let recordPoint = InfluxDBClient
        .Point("demo")
        .addTag(key: "type", value: "point")
        .addField(key: "value", value: .int(2))
//
// Record defined as Data Point with Timestamp
//
let recordPointDate = InfluxDBClient
        .Point("demo")
        .addTag(key: "type", value: "point-timestamp")
        .addField(key: "value", value: .int(2))
        .time(time: .date(Date()))

client.makeWriteAPI().write(points: [recordPoint, recordPointDate]) { result, error in
    // For handle error
    if let error = error {
        print("Error:\n\n\(error)")
    }

    // For Success write
    if result != nil {
        print("Successfully written data:\n\n\([recordPoint, recordPointDate])")
    }
}

//
// Record defined as Tuple
//
let recordTuple: InfluxDBClient.Point.Tuple
        = (measurement: "demo", tags: ["type": "tuple"], fields: ["value": .int(3)], time: nil)

client.makeWriteAPI().write(tuple: recordTuple) { result, error in
    // For handle error
    if let error = error {
        print("Error:\n\n\(error)")
    }

    // For Success write
    if result != nil {
        print("Successfully written data:\n\n\(recordTuple)")
    }
}
```

##### Execute a Flux query

```swift
// Flux query
let query = """
            from(bucket: "\(self.bucket)")
                |> range(start: -10m)
                |> filter(fn: (r) => r["_measurement"] == "cpu")
                |> filter(fn: (r) => r["cpu"] == "cpu-total")
                |> filter(fn: (r) => r["_field"] == "usage_user" or r["_field"] == "usage_system")
                |> last()
            """

print("\nQuery to execute:\n\n\(query)")

client.getQueryAPI().query(query: query) { response, error in
  // For handle error
  if let error = error {
    print("Error:\n\n\(error)")
  }

  // For Success response
  if let response = response {

    print("\nSuccess response...\n")
    print("CPU usage:")
    do {
      try response.forEach { record in
        print("\t\(record.values["_field"]!): \(record.values["_value"]!)")
      }
    } catch {
       print("Error:\n\n\(error)")
    }
  }
}
```
