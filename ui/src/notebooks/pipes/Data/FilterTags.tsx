// Libraries
import React, {FC, useContext} from 'react'

// Components
import {Label as LabelComponent} from '@influxdata/clockface'
import {PipeContext} from 'src/notebooks/context/pipe'

const constructFilters = (value: string, type: string) => {
  if (!value) {
    return null
  }
  switch (type) {
    case 'measurement': {
      return {
        id: value,
        name: `measurement = ${value}`,
        properties: {
          color: 'hotpink',
          description: '',
        },
        type,
      }
    }
    case 'field': {
      return {
        id: value,
        name: `field = ${value}`,
        properties: {
          color: 'lightskyblue',
          description: '',
        },
        type,
      }
    }
    case 'tags': {
      const [tagName] = Object.keys(value)
      const [tagValues] = Object.values(value)
      if (tagName && tagValues) {
        return {
          id: JSON.stringify(value),
          name: `${tagName} = ${tagValues[0]}`,
          properties: {
            color: 'limegreen',
            description: '',
          },
          type,
        }
      }
      return null
    }
    default: {
      return null
    }
  }
}

const FilterTags: FC = () => {
  const {data, update} = useContext(PipeContext)
  const handleDeleteFilter = (type: string) => {
    if (type === 'tags') {
      update({tags: {}})
    } else {
      update({[type]: ''})
    }
  }
  const currentFilters = () => {
    const measurement = constructFilters(data.measurement, 'measurement')
    const filters = []
    if (measurement) {
      filters.push(measurement)
    }
    const field = constructFilters(data.field, 'field')
    if (field) {
      filters.push(field)
    }
    const tags = constructFilters(data.tags, 'tags')
    if (tags) {
      filters.push(tags)
    }
    if (filters.length) {
      return filters.map(_filter => {
        const f = {..._filter}

        return (
          <LabelComponent
            id={f.id}
            key={f.id}
            name={f.name}
            color={f.properties.color}
            description={f.properties.description}
            onDelete={() => handleDeleteFilter(f.type)}
          />
        )
      })
    }
    return <span />
  }
  return (
    <div className="inline-labels">
      <div className="inline-labels--container">
        Filters:&nbsp;{currentFilters()}
      </div>
    </div>
  )
}

export default FilterTags
