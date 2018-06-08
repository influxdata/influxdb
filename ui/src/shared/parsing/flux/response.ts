import Papa from 'papaparse'
import _ from 'lodash'
import uuid from 'uuid'

import {FluxTable} from 'src/types'

export const parseResponse = (response: string): FluxTable[] => {
  const trimmedReponse = response.trim()

  if (_.isEmpty(trimmedReponse)) {
    return []
  }

  return trimmedReponse.split(/\n\s*\n/).reduce((acc, chunk) => {
    return [...acc, ...parseTables(chunk)]
  }, [])
}

export const parseTables = (responseChunk: string): FluxTable[] => {
  const lines = responseChunk.split('\n')
  const annotationLines: string = lines
    .filter(line => line.startsWith('#'))
    .join('\n')
  const nonAnnotationLines: string = lines
    .filter(line => !line.startsWith('#'))
    .join('\n')

  if (_.isEmpty(annotationLines)) {
    throw new Error('Unable to extract annotation data')
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
    _.groupBy<TableGroup[]>(
      nonAnnotationData.slice(1),
      row => row[tableColIndex]
    )
  )

  const partitionRow = annotationData.find(row => row[0] === '#partition')
  const defaultsRow = annotationData.find(row => row[0] === '#default')

  // partitionRow = ['#partition', 'false', 'true', 'true', 'false']
  const partitionKeyIndices = partitionRow.reduce((acc, value, i) => {
    if (value === 'true') {
      return [...acc, i]
    }

    return acc
  }, [])

  const tables = tablesData.map(tableData => {
    const dataRow = _.get(tableData, '0', defaultsRow)
    const partitionKey = partitionKeyIndices.reduce((acc, i) => {
      return {...acc, [headerRow[i]]: _.get(dataRow, i, '')}
    }, {})

    const name = Object.entries(partitionKey)
      .filter(([k]) => !['_start', '_stop'].includes(k))
      .map(([k, v]) => `${k}=${v}`)
      .join(' ')

    return {
      id: uuid.v4(),
      data: [[...headerRow], ...tableData],
      name,
      partitionKey,
    }
  })

  return tables
}
