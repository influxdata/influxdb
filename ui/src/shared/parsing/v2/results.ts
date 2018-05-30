import Papa from 'papaparse'
import _ from 'lodash'
import uuid from 'uuid'

import {ScriptResult} from 'src/types'

export const parseResults = (response: string): ScriptResult[] => {
  const trimmedReponse = response.trim()

  if (_.isEmpty(trimmedReponse)) {
    return []
  }

  return trimmedReponse.split(/\n\s*\n/).map(parseResult)
}

export const parseResult = (raw: string, index: number): ScriptResult => {
  const lines = raw.split('\n')
  const rawMetadata: string = lines
    .filter(line => line.startsWith('#'))
    .map(line => line.slice(1))
    .join('\n')
  const rawData: string = lines.filter(line => !line.startsWith('#')).join('\n')

  const metadata = Papa.parse(rawMetadata).data
  const data = Papa.parse(rawData).data

  const name = `Result ${index}`

  return {
    id: uuid.v4(),
    name,
    data,
    metadata,
  }
}
