// Libraries
import React, {FC, useContext, useState} from 'react'

// Components
import {InfluxColors, Input, List} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'
import FilterTags from 'src/notebooks/pipes/Data/FilterTags'
import MeasurementSelector from 'src/notebooks/pipes/Data/MeasurementSelector'
import FieldSelector from 'src/notebooks/pipes/Data/FieldSelector'
import TagSelector from 'src/notebooks/pipes/Data/TagSelector'

type Props = {
  schema: any
}

const Selectors: FC<Props> = ({schema}) => {
  const {data} = useContext(PipeContext)
  const [searchTerm, setSearchTerm] = useState('')
  const selectedMeasurement = data.measurement
  const selectedField = data.field
  const selectedTags = data?.tags

  const body = (
    <List
      className="data-source--list"
      backgroundColor={InfluxColors.Obsidian}
      maxHeight="300px"
      style={{height: '300px'}}
    >
      {Object.entries(schema)
        .filter(([measurement, values]) => {
          if (!!selectedMeasurement) {
            // filter out non-selected measurements
            return measurement === selectedMeasurement
          }
          const {fields, tags} = values
          if (!!selectedField) {
            // filter out measurements that are not associated with the selected field
            return fields.some(field => field === selectedField)
          }
          if (Object.keys(selectedTags)?.length > 0) {
            const tagNames = Object.keys(selectedTags)
            // TODO(ariel): do we care about matching the values as well?
            return tagNames.some(tagName => tagName in tags)
          }
          if (measurement.includes(searchTerm)) {
            return true
          }
          return fields.some(field => field.includes(searchTerm))
        })
        .map(([measurement, values]) => {
          const {fields, tags} = values
          return (
            <React.Fragment key={measurement}>
              <MeasurementSelector measurement={measurement} />
              <div className="data-subset--list">
                {fields
                  .filter(field => {
                    if (!!selectedField) {
                      return field === selectedField
                    }
                    return field.includes(searchTerm)
                  })
                  .map(field => (
                    <FieldSelector key={field} field={field} />
                  ))}
              </div>
              <div className="data-subset--list">
                {Object.entries(tags)
                  .filter(([tagName, tagValues]) => {
                    const values = tagValues as any[]
                    if (tagName.includes(searchTerm)) {
                      return true
                    }
                    return values?.some(val => val.includes(searchTerm))
                  })
                  .map(([tagName, tagValues]) => {
                    const values = tagValues as any[]
                    return (
                      <React.Fragment key={tagName}>
                        {values
                          .filter(tagValue => tagValue.includes(searchTerm))
                          .map(tagValue => (
                            <TagSelector
                              key={tagValue}
                              tagName={tagName}
                              tagValue={tagValue}
                            />
                          ))}
                      </React.Fragment>
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

export default Selectors
