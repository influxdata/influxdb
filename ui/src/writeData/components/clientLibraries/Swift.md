For more detailed and up to date information check out the [GitHub Repository](https://github.com/influxdata/influxdb-client-swift/)

##### Install via Swift Package Manager

Add this line to your `Package.swift`:

```swift
// swift-tools-version:5.3
import PackageDescription

let package = Package(
    name: "MyPackage",
    dependencies: [
        .package(url: "https://github.com/influxdata/influxdb-client-swift", from: "VERSION.STRING.HERE"),
    ],
    targets: [
        .target(name: "MyModule", dependencies: ["InfluxDBSwift"])
    ]
)
```

##### Creating a client

```swift
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

##### Execute a Flux query
