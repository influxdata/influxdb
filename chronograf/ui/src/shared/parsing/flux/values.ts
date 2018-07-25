import _ from 'lodash'

import {FluxTable} from 'src/types'
import {parseResponse} from 'src/shared/parsing/flux/response'

const parseValuesColumn = (resp: string): string[] => {
  const results = parseResponse(resp)

  if (results.length === 0) {
    return []
  }

  const tags = results.reduce<string[]>((acc, result: FluxTable) => {
    const colIndex = result.data[0].findIndex(header => header === '_value')

    if (colIndex === -1) {
      return [...acc]
    }

    const resultTags = result.data.slice(1).map(row => row[colIndex])

    return [...acc, ...resultTags]
  }, [])

  return _.sortBy(tags, t => t.toLocaleLowerCase())
}

export default parseValuesColumn
