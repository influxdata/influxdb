/*
  Given Flux query response as a CSV, check if the CSV contains an error table
  as the first result. If it does, throw the error message contained within
  that table. 

  For example, given the following response:

      #datatype,string,long
      ,error,reference
      ,Failed to parse query,897

  we want to throw an error with the message "Failed to parse query".

  See https://github.com/influxdata/flux/blob/master/docs/SPEC.md#errors.
*/
export const checkQueryResult = (file: string = ''): void => {
  // Don't check the whole file, since it could be huge and the error table
  // will be within the first few lines (if it exists)
  const fileHead = file.slice(0, findNthIndex(file, '\n', 6))

  const lines = fileHead.split('\n').filter(line => !line.startsWith('#'))

  if (!lines.length || !lines[0].includes('error') || !lines[1]) {
    return
  }

  const header = lines[0].split(',').map(s => s.trim())
  const row = lines[1].split(',').map(s => s.trim())
  const index = header.indexOf('error')

  if (index === -1 || !row[index]) {
    return
  }

  // Trim off extra quotes at start and end of message
  const errorMessage = row[index].replace(/^"/, '').replace(/"$/, '')

  throw new Error(errorMessage)
}

const findNthIndex = (s: string, c: string, n: number) => {
  let count = 0
  let i = 0

  while (i < s.length) {
    if (s[i] == c) {
      count += 1
    }

    if (count === n) {
      return i
    }

    i += 1
  }
}
