// Libraries
import React, {FC, useState, useContext} from 'react'
import {uniq, get} from 'lodash'
// Types
import {PipeProp} from 'src/notebooks'

// Contexts
import BucketProvider from 'src/notebooks/context/buckets'
import {QueryContext} from 'src/notebooks/context/query'
import {PipeContext} from 'src/notebooks/context/pipe'

// Components
import BucketSelector from 'src/notebooks/pipes/Data/BucketSelector'
import Selectors from 'src/notebooks/pipes/Data/Selectors'
import Schema from 'src/notebooks/pipes/Data/Schema'
import {FlexBox, ComponentSize} from '@influxdata/clockface'

// Styles
import 'src/notebooks/pipes/Query/style.scss'

const DataSource: FC<PipeProp> = ({Context}) => {
  const {data} = useContext(PipeContext)
  const {query} = useContext(QueryContext)
  const [schema, setSchema] = useState<any>(null)
  const handleClick = (): void => {
    const fetchSchema = async () => {
      const text = `import "influxdata/influxdb/v1"
from(bucket: "${data.bucketName}")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> first()
  |> v1.fieldsAsCols()`
      const result = await query(text)
      const parsedTable = result.parsed.table

      const keys = parsedTable.columnKeys

      const simpleSchema = keys.reduce((acc, k) => {
        const keyValues = uniq(
          get(parsedTable, `columns[${k}].data`, [])
        ).filter(value => value !== undefined) as string[]

        const name = get(parsedTable, `columns[${k}].name`)
        acc[name] = keyValues
        return acc
      }, {})
      // TODO(ariel): store this in context so that we don't have to requery
      setSchema(simpleSchema)
    }

    fetchSchema()
  }

  const resetSchema = (): void => {
    setSchema(null)
  }

  return (
    <BucketProvider>
      <Context>
        <FlexBox
          margin={ComponentSize.Large}
          stretchToFitWidth={true}
          className="data-source"
        >
          <BucketSelector resetSchema={resetSchema} />
          {schema === null ? (
            <Schema handleClick={handleClick} />
          ) : (
            <Selectors schema={schema} />
          )}
        </FlexBox>
      </Context>
    </BucketProvider>
  )
}

export default DataSource
