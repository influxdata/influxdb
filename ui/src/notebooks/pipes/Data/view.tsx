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
import MeasurementSelector from 'src/notebooks/pipes/Data/MeasurementSelector'
import FieldSelector from 'src/notebooks/pipes/Data/FieldSelector'
import TagSelector from 'src/notebooks/pipes/Data/TagSelector'
import Schema from 'src/notebooks/pipes/Data/Schema'
import {FlexBox, ComponentSize} from '@influxdata/clockface'

// Styles
import 'src/notebooks/pipes/Query/style.scss'
// import {SelectorProvider} from 'src/notebooks/context/schemaSelectors'
import {results} from './dummyData'

const DataSource: FC<PipeProp> = ({Context}) => {
  const {data} = useContext(PipeContext)
  const {query} = useContext(QueryContext)
  const [schema, setSchema] = useState<any>(null)
  const [measurements, setMeasurements] = useState<any>(null)
  const [fields, setFields] = useState<any>(null)
  const handleClick = (): void => {
    const fetchSchema = async () => {
      const bucket = data.bucketName
      const m = Object.keys(results[bucket])
      console.log('results[bucket]: ', results[bucket])
      console.log('object: ', Object.values(results[bucket]))
      console.log('m: ', m)
      // const simpleSchema = keys.reduce((acc, k) => {
      //   const keyValues = uniq(
      //     get(parsedTable, `columns[${k}].data`, [])
      //   ).filter(value => value !== undefined) as string[]

      //   const name = get(parsedTable, `columns[${k}].name`)
      //   acc[name] = keyValues
      //   return acc
      // }, {})

      setMeasurements(m)
      setFields(results[bucket])
      // setFields(simpleSchema['_field'])

      // console.log('measurements ', measurements)
      // console.log('fields: ', fields)
      // console.log('simpleSchema; ', simpleSchema)

      // // TODO(ariel): store this in context so that we don't have to requery
      setSchema(results)
    }

    fetchSchema()
  }

  const resetSchema = (): void => {
    setSchema(null)
  }

  let body = <span />

  if (measurements !== null && fields !== null) {
    body = (
      <>
        <MeasurementSelector schema={measurements} />
        <FieldSelector schema={fields} />
        <TagSelector schema={fields} />
      </>
    )
  }

  if (measurements !== null && fields === null) {
    body = (
      <>
        <MeasurementSelector schema={measurements} />
      </>
    )
  }

  return (
    <BucketProvider>
      {/*<SelectorProvider>*/}
      <Context>
        <FlexBox
          margin={ComponentSize.Large}
          stretchToFitWidth={true}
          className="data-source"
        >
          <BucketSelector resetSchema={resetSchema} />
          {schema === null ? <Schema handleClick={handleClick} /> : <>{body}</>}
        </FlexBox>
      </Context>
      {/*</SelectorProvider>*/}
    </BucketProvider>
  )
}

export default DataSource
