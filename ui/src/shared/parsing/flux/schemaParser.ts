import Papa from 'papaparse'
import {get, groupBy, isEmpty} from 'lodash'

import {Schema} from 'src/types'

/*
  A Flux CSV response can contain multiple CSV files each joined by a newline.
  This function splits up a CSV response into these individual CSV files.

  See https://github.com/influxdata/flux/blob/master/docs/SPEC.md#multiple-tables.
*/
export const parseChunks = (response: string): string[] => {
  const trimmedResponse = response.trim()

  if (trimmedResponse === '') {
    return []
  }

  // Split the response into separate chunks whenever we encounter:
  //
  // 1. A newline
  // 2. Followed by any amount of whitespace
  // 3. Followed by a newline
  // 4. Followed by a `#` character
  //
  // The last condition is [necessary][0] for handling CSV responses with
  // values containing newlines.
  //
  // [0]: https://github.com/influxdata/influxdb/issues/15017

  const chunks = trimmedResponse
    .split(/\n\s*\n#/)
    .map((s, i) => (i === 0 ? s : `#${s}`))

  return chunks
}

export const parseResponse = (response: string): Schema => {
  const schema: Schema = {}
  const chunks = parseChunks(response)
  chunks.forEach(chunk => parseChunk(chunk, schema))
  return schema
}

const formatSchemaByGroupKey = (groupKey, schema: Schema) => {
  const measurement = groupKey['_measurement']

  if (!measurement) {
    return {}
  }
  const field = groupKey['_field']
  const tags = Object.entries(groupKey)
    .filter(([k]) => {
      return !(
        k === '_start' ||
        k === '_stop' ||
        k === '_measurement' ||
        k === '_field' ||
        k === 'undefined'
      )
    })
    .reduce((acc, [k, v]) => {
      if (k !== undefined && v !== undefined) {
        if (acc[k]) {
          acc[k] = [...acc[k], v]
        } else {
          acc[k] = [v]
        }
      }
      return acc
    }, {})

  if (schema[measurement]) {
    let fields = schema[measurement].fields || []
    if (field && !fields.includes(field)) {
      fields = fields.concat(field)
    }
    const existingTags = schema[measurement].tags || {}
    Object.entries(existingTags).forEach(([k, values]) => {
      if (tags[k]) {
        let tagValues = tags[k]
        values.forEach(value => {
          if (!tagValues.includes(value)) {
            tagValues = tagValues.concat(value)
          }
        })
        tags[k] = tagValues
      }
    })
    schema[measurement] = {
      fields,
      tags,
      type: 'string',
    }
  } else {
    schema[measurement] = {
      type: 'string',
      fields: field ? [field] : [],
      tags,
    }
  }
}

export const parseChunk = (responseChunk: string, schema: Schema) => {
  const lines = responseChunk.split('\n')
  const annotationLines: string = lines
    .filter(line => line.startsWith('#'))
    .join('\n')
    .trim()
  const nonAnnotationLines: string = lines
    .filter(line => !line.startsWith('#'))
    .join('\n')
    .trim()

  if (isEmpty(annotationLines)) {
    throw new Error('Unable to extract annotation data')
  }

  if (isEmpty(nonAnnotationLines)) {
    // A response may be truncated on an arbitrary line. This guards against
    // the case where a response is truncated on annotation data
    return []
  }

  const nonAnnotationData = Papa.parse(nonAnnotationLines).data
  const annotationData = Papa.parse(annotationLines).data
  const headerRow = nonAnnotationData[0]
  const tableColIndex = headerRow.findIndex(h => h === 'table')

  interface TableGroup {
    [tableId: string]: string[]
  }

  // Group rows by their table id
  const tablesData = Object.values(
    groupBy<TableGroup[]>(nonAnnotationData.slice(1), row => row[tableColIndex])
  )

  const groupRow = annotationData.find(row => row[0] === '#group')
  const defaultsRow = annotationData.find(row => row[0] === '#default')

  // groupRow = ['#group', 'false', 'true', 'true', 'false']
  const groupKeyIndices = groupRow.reduce((acc, value, i) => {
    if (value === 'true') {
      return [...acc, i]
    }

    return acc
  }, [])

  tablesData.forEach(tableData => {
    const dataRow = get(tableData, '0', defaultsRow)

    const groupKey = groupKeyIndices.reduce((acc, i) => {
      return {...acc, [headerRow[i]]: get(dataRow, i, '')}
    }, {})

    formatSchemaByGroupKey(groupKey, schema)
  })
}
