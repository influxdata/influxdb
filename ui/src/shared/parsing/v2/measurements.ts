import {parseResponse} from 'src/shared/parsing/v2/results'

const parseMeasurements = (resp: string): string[] => {
  const results = parseResponse(resp)

  if (results.length === 0) {
    return []
  }

  const result = results[0]
  const colIndex = result.data[0].findIndex(col => col === '_measurement')

  if (!colIndex) {
    throw new Error('Unexpected metaquery result')
  }

  return result.data.slice(1).map(row => row[colIndex])
}

export default parseMeasurements
