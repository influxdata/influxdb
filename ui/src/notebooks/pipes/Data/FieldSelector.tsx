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
  // TODO(ariel): refactor so that the measurement and fields are separate
  const {data, update} = useContext(PipeContext)
  // const {buckets, loading} = useContext(BucketContext)
  const [searchTerm, setSearchTerm] = useState('')
  const selectedField = data.field
  const selectedMeasurement = data.measurement

  const updateSelection = useCallback(
    (value: string): void => {
      let updated = value
      let selectedTags = data?.tags
      if (updated === selectedField) {
        updated = ''
        selectedTags = {}
      }
      update({field: updated, tags: selectedTags})
    },
    [update]
  )

  // TODO(ariel): add some context

  let body

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
  if (/*loading === RemoteDataState.Done*/ selectedMeasurement) {
    body = (
      <List
        className="data-source--list"
        backgroundColor={InfluxColors.Obsidian}
      >
        {schema[selectedMeasurement].fields
          .filter(name => name.includes(searchTerm))
          .map(name => (
            <List.Item
              key={name}
              value={name}
              onClick={() => updateSelection(name)}
              selected={name === selectedField}
              title={name}
              gradient={Gradients.GundamPilot}
              wrapText={true}
            >
              <List.Indicator type="dot" />
              {name}
            </List.Item>
          ))}
      </List>
    )
  }

  return (
    <div className="data-source--block">
      <div className="data-source--block-title">Field</div>
      <Input
        value={searchTerm}
        placeholder="Search for a field"
        className="tag-selector--search"
        onChange={e => setSearchTerm(e.target.value)}
      />
      {body}
    </div>
  )
}

export default FieldSelector
