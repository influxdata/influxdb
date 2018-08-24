import Papa from 'papaparse'
import _ from 'lodash'
import uuid from 'uuid'

import {FluxTable} from 'src/types'

export const parseResponseError = (response: string): FluxTable[] => {
  const data = Papa.parse(response.trim()).data as string[][]

  return [
    {
      id: uuid.v4(),
      name: 'Error',
      groupKey: {},
      data,
    },
  ]
}

export const parseResponse = (response: string): FluxTable[] => {
  const trimmedResponse = response.trim()

  if (_.isEmpty(trimmedResponse)) {
    return []
  }

  return trimmedResponse.split(/\n\s*\n/).reduce((acc, chunk) => {
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

  if (_.isEmpty(nonAnnotationLines)) {
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
    _.groupBy<TableGroup[]>(
      nonAnnotationData.slice(1),
      row => row[tableColIndex]
    )
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

  const tables = tablesData.map(tableData => {
    const dataRow = _.get(tableData, '0', defaultsRow)
    const groupKey = groupKeyIndices.reduce((acc, i) => {
      return {...acc, [headerRow[i]]: _.get(dataRow, i, '')}
    }, {})

    const name = Object.entries(groupKey)
      .filter(([k]) => !['_start', '_stop'].includes(k))
      .map(([k, v]) => `${k}=${v}`)
      .join(' ')

    return {
      id: uuid.v4(),
      data: [[...headerRow], ...tableData],
      name,
      groupKey,
    }
  })

  return tables
}
