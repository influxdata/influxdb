// Libraries
import React, {FC, useState, useContext} from 'react'
import {uniq, get} from 'lodash'

// Contexts
import {QueryContext} from 'src/notebooks/context/query'
import {PipeContext} from 'src/notebooks/context/pipe'

const SchemaFetcher: FC = () => {
  const {data} = useContext(PipeContext)
  const {query} = useContext(QueryContext)
  const [schema, setSchema] = useState<any>(null)

  const handleClick = () => {
    const fetchSchema = async () => {
      //   const text = `from(bucket: "${data.bucketName}")
      // |> range(start: -10m, stop: now())
      // |> keys()
      // |> drop(columns: ["_start","_stop"])
      // |> limit(n:1)
      // |> pivot(valueColumn: "_value", rowKey: ["_measurement"], columnKey: ["_field"])`
      const text = `import "influxdata/influxdb/v1"
from(bucket: "${data.bucketName}")
    |> range(start: -10s)
    |> last()
    |> drop(columns: ["_start","_stop"])
    |> v1.fieldsAsCols()`
      const result = await query(text)
      const parsedTable = result.parsed.table

      // const keys = parsedTable.columnKeys

      // const simpleSchema = keys.map(k => {
      //   const keyValues: string[] = uniq(
      //     get(parsedTable, `columns[${k}].data`, [])
      //   )
      //   const name = get(parsedTable, `columns[${k}].name`)

      //   return {
      //     name,
      //     keyValues,
      //   }
      // })

      setSchema(result)
    }

    fetchSchema()
  }

  console.log(schema)

  return <button onClick={handleClick}>Fetch Schema</button>
}

export default SchemaFetcher
