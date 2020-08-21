// Types
import {PipeData} from 'src/notebooks'

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
        cache[tagName] = vals
        set.add(vals)
        results = results.concat({
          [tagName]: values,
        })
        return
      }
      if (cache[tagName] === vals || set.has(vals)) {
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
    return field.toLowerCase().includes(searchTerm)
  })
}

const filterTags = (tags: any[], searchTerm: string): any[] =>
  tags.filter(
    tag =>
      Object.entries(tag).filter(([tagName, tagValues]) => {
        const values = tagValues as any[]
        if (tagName.toLowerCase().includes(searchTerm)) {
          return true
        }
        return values?.some(val => val.toLowerCase().includes(searchTerm))
      }).length !== 0
  )

const dedupeArray = (array: string[]): string[] => {
  const cache = {}
  return array.filter(m => {
    if (m in cache) {
      return false
    }
    cache[m] = true
    return true
  })
}

export const normalizeSchema = (
  schema: any,
  data: PipeData,
  searchTerm: string
) => {
  const lowerCasedSearchTerm = searchTerm.toLowerCase()
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
      if (measurement.toLowerCase().includes(lowerCasedSearchTerm)) {
        return true
      }
      if (
        fields.some(field => field.toLowerCase().includes(lowerCasedSearchTerm))
      ) {
        return true
      }
      return Object.entries(tags).some(([tag, tagValues]) => {
        if (tag.toLowerCase().includes(lowerCasedSearchTerm)) {
          return true
        }
        return (
          tagValues?.some(tagValue =>
            tagValue.toLowerCase().includes(lowerCasedSearchTerm)
          ) || false
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
    dedupeArray(fieldResults),
    lowerCasedSearchTerm,
    data?.field
  )

  const filteredTags = filterTags(dedupedTags, lowerCasedSearchTerm)

  return {
    measurements: dedupeArray(measurements),
    fields: filteredFields,
    tags: filteredTags,
  }
}
