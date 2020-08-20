// Libraries
import {isEqual, uniq} from 'lodash'

export const dedupeTags = (tags: any[]) => {
  const cache = {}
  const set = new Set()
  let results = []
  tags.forEach(tag => {
    Object.entries(tag).forEach(([tagName, tagValues]) => {
      // sorting tagValues to check for exact matches when doing string comparisons
      const values = tagValues as string[]
      values.sort()
      const vals = JSON.stringify(values)
      if (tagName in cache === false) {
        cache[tagName] = values
        set.add(vals)
        results = results.concat({
          [tagName]: values,
        })
        return
      }
      if (isEqual(cache[tagName], values) || set.has(vals)) {
        return
      } else {
        set.add(vals)
        results = results.concat({
          [tagName]: values,
        })
      }
    })
  })
  return results
}

const filterFields = (
  fields: string[],
  searchTerm: string,
  selectedField: string
): string[] => {
  return fields.filter((field: string) => {
    if (!!selectedField) {
      return field === selectedField
    }
    return field.includes(searchTerm)
  })
}

const filterTags = (tags: any[], searchTerm: string): any[] =>
  tags.filter(
    tag =>
      Object.entries(tag).filter(([tagName, tagValues]) => {
        const values = tagValues as any[]
        if (tagName.includes(searchTerm)) {
          return true
        }
        return values?.some(val => val.includes(searchTerm))
      }).length !== 0
  )

export const normalizeSchema = (schema: any, data: any, searchTerm: string) => {
  const selectedMeasurement = data.measurement
  const selectedField = data.field
  const selectedTags = data?.tags
  const measurements = []
  let fieldResults = []
  let tagResults = []
  Object.entries(schema)
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
      if (fields.some(field => field.includes(searchTerm))) {
        return true
      }
      return Object.entries(tags).some(([tag, tagValues]) => {
        if (tag.includes(searchTerm)) {
          return true
        }
        return (
          tagValues?.some(tagValue => tagValue.includes(searchTerm)) || false
        )
      })
    })
    .forEach(([measurement, values]) => {
      measurements.push(measurement)
      const {fields, tags} = values
      fieldResults = fieldResults.concat(fields)
      tagResults = tagResults.concat(tags)
    })

  const dedupedTags = dedupeTags(tagResults)
  const filteredFields = filterFields(
    uniq(fieldResults),
    searchTerm,
    data?.field
  )

  const filteredTags = filterTags(dedupedTags, searchTerm)

  return {
    measurements: uniq(measurements),
    fields: filteredFields,
    tags: filteredTags,
  }
}
