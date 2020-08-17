// Libraries
import React, {FC, useContext, useCallback, useState} from 'react'

// Components
import {InfluxColors, Input, List, Gradients} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'
import FilterTags from 'src/notebooks/pipes/Data/FilterTags'

type Props = {
  schema: any
}

const MeasurementSelector: FC<Props> = ({schema}) => {
  const {data, update} = useContext(PipeContext)
  const [searchTerm, setSearchTerm] = useState('')
  const selectedMeasurement = data.measurement
  const updateMeasurementSelection = useCallback(
    (value: string): void => {
      let updated = value
      if (updated === selectedMeasurement) {
        updated = ''
      }

      update({measurement: updated})
    },
    [update]
  )

  const selectedField = data.field
  const updateFieldSelection = useCallback(
    (field: string): void => {
      let updated = field
      if (updated === selectedField) {
        updated = ''
      }
      update({field: updated})
    },
    [update]
  )

  const selectedTags = data?.tags

  const handleSublistMultiSelect = useCallback(
    (tagName: string, tagValue: string): void => {
      const tagValues = [...selectedTags[tagName], tagValue]
      // do a multi select deselect
      update({
        tags: {
          ...selectedTags,
          [tagName]: tagValues,
        },
      })
    },
    [update]
  )

  const handleSubListItemClick = useCallback(
    (event: MouseEvent, tagName: string, tagValue: string) => {
      if (event.metaKey) {
        handleSublistMultiSelect(tagName, tagValue)
        return
      }
      let updatedValue = [tagValue]
      let tags = {
        [tagName]: updatedValue,
      }
      if (selectedTags[tagName]?.includes(tagValue)) {
        updatedValue = []
        tags[tagName] = updatedValue
      }
      if (tagName in selectedTags && updatedValue.length === 0) {
        tags = {}
      }
      update({
        tags,
      })
    },
    [update]
  )

  const body = (
    <List className="data-source--list" backgroundColor={InfluxColors.Obsidian}>
      {Object.entries(schema)
        .filter(([measurement, values]) => {
          if (!!selectedMeasurement) {
            return measurement === selectedMeasurement
          }
          const {fields} = values
          if (measurement.includes(searchTerm)) {
            return true
          }
          return fields.some(field => field)
        })
        .map(([measurement, values]) => {
          const {fields, tags} = values
          return (
            <React.Fragment key={measurement}>
              <List.Item
                key={measurement}
                value={measurement}
                onClick={() => updateMeasurementSelection(measurement)}
                selected={measurement === selectedMeasurement}
                title={measurement}
                gradient={Gradients.GundamPilot}
                wrapText={true}
              >
                <List.Indicator type="dot" />
                <div className="data-measurement--equation">
                  {`_measurement = ${measurement}`}
                </div>
                <div className="data-measurement--name">
                  &nbsp;measurement&nbsp;
                </div>
                <div className="data-measurement--type">string</div>
              </List.Item>
              <div className="data-subset--list">
                {fields
                  .filter(field => {
                    if (!!selectedField) {
                      return field === selectedField
                    }
                    return field.includes(searchTerm)
                  })
                  .map(field => (
                    <List.Item
                      key={field}
                      value={field}
                      onClick={() => updateFieldSelection(field)}
                      selected={field === selectedField}
                      title={field}
                      gradient={Gradients.GundamPilot}
                      wrapText={true}
                    >
                      <List.Indicator type="dot" />
                      <div className="data-field--equation">
                        {`_field = ${field}`}
                      </div>
                      <div className="data-measurement--name">
                        &nbsp;field&nbsp;
                      </div>
                      <div className="data-measurement--type">string</div>
                    </List.Item>
                  ))}
              </div>
              <div className="data-subset--list">
                {Object.entries(tags)
                  .filter(([tagName, tagValues]) => {
                    const values = tagValues as any[]
                    if (Object.keys(selectedTags).length) {
                      return selectedTags[tagName]
                    }
                    if (tagName.includes(searchTerm)) {
                      return true
                    }
                    return values?.some(val => val.includes(searchTerm))
                  })
                  .map(([tagName, tagValues]) => {
                    const values = tagValues as any[]
                    return (
                      <div className="data-subset--list">
                        {values
                          .filter(val => {
                            if (Object.keys(selectedTags).length > 0) {
                              return selectedTags[tagName] === val
                            }
                            return val.includes(searchTerm)
                          })
                          .map(val => (
                            <List.Item
                              key={val}
                              value={val}
                              onClick={(event: MouseEvent) =>
                                handleSubListItemClick(event, tagName, val)
                              }
                              selected={selectedTags[tagName]?.includes(val)}
                              title={val}
                              gradient={Gradients.GundamPilot}
                              wrapText={true}
                            >
                              <List.Indicator type="dot" />
                              <div className="data-tag--equation">
                                {`${tagName} = ${val}`}
                              </div>
                              <div className="data-measurement--name">
                                &nbsp;tag key&nbsp;
                              </div>
                              <div className="data-measurement--type">
                                string
                              </div>
                            </List.Item>
                          ))}
                      </div>
                    )
                  })}
              </div>
            </React.Fragment>
          )
        })}
    </List>
  )

  return (
    <div className="data-source--block-results">
      <div className="data-source--block-title">
        <FilterTags />
      </div>
      <Input
        value={searchTerm}
        placeholder="Type to filter by Measurement, Field, or Tag ..."
        className="tag-selector--search"
        onChange={e => setSearchTerm(e.target.value)}
      />
      {body}
    </div>
  )
}

export default MeasurementSelector
