// Libraries
import React, {FC, useContext} from 'react'

// Components
import {
  TechnoSpinner,
  ComponentSize,
  RemoteDataState,
} from '@influxdata/clockface'
import Schema from 'src/notebooks/pipes/Data/Schema'
import MeasurementSelector from 'src/notebooks/pipes/Data/MeasurementSelector'
import {SchemaContext} from 'src/notebooks/context/schemaProvider'

const FieldsList: FC = () => {
  const {loading, schema} = useContext(SchemaContext)

  let body = <Schema />

  if (loading === RemoteDataState.Loading) {
    body = (
      <div className="data-source--list__empty">
        <TechnoSpinner strokeWidth={ComponentSize.Small} diameterPixels={32} />
      </div>
    )
  }

  if (loading === RemoteDataState.Error) {
    body = (
      <div className="data-source--list__empty">
        <p>Could not fetch schema</p>
      </div>
    )
  }

  if (loading === RemoteDataState.Done && Object.keys(schema).length > 0) {
    body = <MeasurementSelector schema={schema.measurements} />
  }

  return body
}

export default FieldsList
