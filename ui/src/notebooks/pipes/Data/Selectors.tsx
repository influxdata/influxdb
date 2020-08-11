// Libraries
import React, {FC, useContext, useCallback, useState} from 'react'

// Components
import {
  // TechnoSpinner,
  // ComponentSize,
  // RemoteDataState,
  InfluxColors,
  Input,
  List,
  Gradients,
} from '@influxdata/clockface'
// import {BucketContext} from 'src/notebooks/context/buckets'
import {PipeContext} from 'src/notebooks/context/pipe'

type Props = {
  schema: any
}

const FieldSelector: FC<Props> = ({schema}) => {
  const {data, update} = useContext(PipeContext)
  // const {buckets, loading} = useContext(BucketContext)
  const [searchTerm, setSearchTerm] = useState('')
  const selectedMeasurement = data.measurement
  const selectedField = data.field

  console.log('data: ', data)

  const updateFieldAndMeasurement = useCallback(
    (measurement: string, field: string): void => {
      let updatedMeasurement = measurement
      let updatedField = field
      if (
        updatedMeasurement === selectedMeasurement &&
        updatedField === selectedField
      ) {
        updatedMeasurement = ''
        updatedField = ''
      }
      update({measurement: updatedMeasurement, field: updatedField})
    },
    [update]
  )

  // TODO(ariel): add some context

  // let body

  // if (loading === RemoteDataState.Loading) {
  //   body = (
  //     <div className="data-source--list__empty">
  //       <TechnoSpinner strokeWidth={ComponentSize.Small} diameterPixels={32} />
  //     </div>
  //   )
  // }

  // if (loading === RemoteDataState.Error) {
  //   body = (
  //     <div className="data-source--list__empty">
  //       <p>Could not fetch schema</p>
  //     </div>
  //   )
  // }

  // if (loading === RemoteDataState.Done) {
  const body = (
    <List className="data-source--list" backgroundColor={InfluxColors.Obsidian}>
      {Object.entries(schema)
        .filter(([name, values]) => {
          const fields = values as any[]
          if (name.includes(searchTerm)) {
            return true
          }
          return fields.some(val => `${val}`.includes(searchTerm))
        })
        .map(([measurement, values]) => {
          const fields = values as any[]
          return (
            <>
              <List.Item
                key={measurement}
                value={measurement}
                onClick={() => updateFieldAndMeasurement(measurement, '')}
                selected={measurement === selectedMeasurement}
                title={measurement}
                gradient={Gradients.GundamPilot}
                wrapText={true}
              >
                <List.Indicator type="dot" />
                {measurement}
              </List.Item>
              <div className="data-subset--list">
                {fields
                  .filter(field => {
                    if (measurement.includes(searchTerm)) {
                      return true
                    }
                    return `${field}`.includes(searchTerm)
                  })
                  .map(field => (
                    <List.Item
                      key={`${measurement}_${field}`}
                      value={field}
                      onClick={() =>
                        updateFieldAndMeasurement(measurement, field)
                      }
                      selected={
                        field === selectedField &&
                        measurement === selectedMeasurement
                      }
                      title={field}
                      gradient={Gradients.GundamPilot}
                      wrapText={true}
                    >
                      <List.Indicator type="dot" />
                      {field}
                    </List.Item>
                  ))}
              </div>
              <List.Divider />
            </>
          )
        })}
    </List>
  )
  // }

  return (
    <div className="data-source--block">
      <div className="data-source--block-title">Measurement & Fields</div>
      <Input
        value={searchTerm}
        placeholder="Search for a bucket"
        className="tag-selector--search"
        onChange={e => setSearchTerm(e.target.value)}
      />
      {body}
    </div>
  )
}

export default FieldSelector
