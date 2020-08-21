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
      const tagNames = Object.keys(value)
      if (tagNames) {
        const tags = []
        tagNames
          .filter(tagName => !!value[tagName])
          .forEach(tagName => {
            const tagValues = value[tagName]
            const mappedTags = tagValues.map(tagValue => ({
              id: tagValue,
              name: `${tagName} = ${tagValue}`,
              properties: {
                color: 'limegreen',
                description: '',
              },
              type,
            }))
            tags.push(...mappedTags)
          })
        return tags
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
  const handleDeleteFilter = (type: string, name: string) => {
    if (type === 'tags') {
      const [tagName, tagValue] = name.split(' = ')
      let tagValues = []
      const selectedTags = data?.tags
      if (!selectedTags[tagName]) {
        tagValues = [tagValue]
      } else if (
        selectedTags[tagName] &&
        selectedTags[tagName].includes(tagValue)
      ) {
        tagValues = selectedTags[tagName].filter(v => v !== tagValue)
      } else {
        tagValues = [...selectedTags[tagName], tagValue]
      }

      let tags = {
        ...selectedTags,
        [tagName]: tagValues,
      }

      if (tagValues.length === 0) {
        tags = {}
      }

      update({
        tags,
      })
    } else {
      update({[type]: ''})
    }
  }
  const currentFilters = () => {
    const measurement = constructFilters(data.measurement, 'measurement')
    let filters = []
    if (measurement) {
      filters = filters.concat(measurement)
    }
    const field = constructFilters(data.field, 'field')
    if (field) {
      filters = filters.concat(field)
    }
    const tags = constructFilters(data.tags, 'tags')
    if (tags) {
      filters = filters.concat(tags)
    }
    if (filters.length) {
      return filters.map(_filter => {
        const f = {..._filter}

        return (
          <LabelComponent
            className="data-source--filter"
            id={f.id}
            key={f.id}
            name={f.name}
            color={f.properties.color}
            description={f.properties.description}
            onDelete={() => handleDeleteFilter(f.type, f.name)}
          />
        )
      })
    }
    return <span />
  }
  return (
    <div className="data-source--filters">
      <p className="data-source--filters-label">Filters:</p>
      <div className="data-source--filters-list">{currentFilters()}</div>
    </div>
  )
}

export default FilterTags
