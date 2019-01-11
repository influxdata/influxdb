import Papa from 'papaparse'

import {parseChunks} from 'src/shared/parsing/flux/response'

export interface ParseFilesResult {
  data: string[][]
  maxColumnCount: number
}

export const parseFiles = (responses: string[]): ParseFilesResult => {
  const chunks = parseChunks(responses.join('\n\n'))
  const parsedChunks = chunks.map(c => Papa.parse(c).data)
  const maxColumnCount = Math.max(...parsedChunks.map(c => c[0].length))
  const data = []

  for (let i = 0; i < parsedChunks.length; i++) {
    if (i !== 0) {
      // Seperate each chunk by an empty line, just like in the unparsed CSV
      data.push([])
    }

    data.push(...parsedChunks[i])

    // Add an empty line at the end
    data.push([])
  }

  return {data, maxColumnCount}
}
