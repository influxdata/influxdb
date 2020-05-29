import _ from 'lodash'
import Papa from 'papaparse'

interface MapResult {
  values: {[key: string]: string}
  errors: string[]
}

export const csvToMap = (csv: string): MapResult => {
  let errors = []
  const trimmed = _.trimEnd(csv, '\n')
  const parsedTVS = Papa.parse(trimmed)
  const templateValuesData: string[][] = _.get(parsedTVS, 'data', [[]])

  if (templateValuesData.length === 0) {
    return {values: {}, errors}
  }

  const keys = new Set<string>()
  const values = {}

  for (const arr of templateValuesData) {
    if (arr.length === 2 || (arr.length === 3 && arr[2] === '')) {
      const key = trimAndRemoveQuotes(arr[0])
      const value = trimAndRemoveQuotes(arr[1])

      if (!keys.has(key) && key !== '') {
        values[key] = value
        keys.add(key)
      }
    } else {
      errors = [...errors, arr[0]]
    }
  }

  return {values, errors}
}

export const trimAndRemoveQuotes = (elt: string): string => {
  const trimmed = elt.trim()
  const dequoted = trimmed.replace(/(^")|("$)/g, '')

  return dequoted
}

export const mapToCSV = (values: {[key: string]: string}): string =>
  Object.entries(values)
    .map(([key, value]) => `${key},"${value}"`)
    .join('\n')
