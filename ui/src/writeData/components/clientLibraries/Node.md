For more detailed and up to date information check out the [GitHub Respository](https://github.com/influxdata/influxdb-client-js)

##### Install via NPM

```
npm i @influxdata/influxdb-client
```

##### Initialize the Client

```
const {InfluxDB} = require('@influxdata/influxdb-client')

// You can generate a Token from the "Tokens Tab" in the UI
const token = '<%= token %>'
const org = '<%= org %>'
const bucket = '<%= bucket %>'

const client = new InfluxDB({url: '<%= server %>', token: token})
```

##### Write Data

```
const {Point} = require('@influxdata/influxdb-client')
const writeApi = client.getWriteApi(org, bucket)
writeApi.useDefaultTags({host: 'host1'})

const point = new Point('mem')
  .floatField('used_percent', 23.43234543)
writeApi.writePoint(point)
writeApi
    .close()
    .then(() => {
        console.log('FINISHED')
    })
    .catch(e => {
        console.error(e)
        console.log('\\nFinished ERROR')
    })
```

##### Execute a Flux query

```
const queryApi = client.getQueryApi(org)

const query = \`from(bucket: \"\${bucket}\") |> range(start: -1h)\`
queryApi.queryRows(query, {
  next(row, tableMeta) {
    const o = tableMeta.toObject(row)
    console.log(
      \`\${o._time} \${o._measurement} in \'\${o.location}\' (\${o.example}): \${o._field}=\${o._value}\`
    )
  },
  error(error) {
    console.error(error)
    console.log('\\nFinished ERROR')
  },
  complete() {
    console.log('\\nFinished SUCCESS')
  },
})
```
