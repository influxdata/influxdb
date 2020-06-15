import Papa from 'papaparse'

const enum InternalTypes {
  boolean = 'boolean',
  number = 'number',
  string = 'string',
  time = 'time',
}
const enum ExternalTypes {
  boolean = 'boolean',
  unsignedLong = 'unsignedLong',
  long = 'long',
  double = 'double',
  string = 'string',
  time = 'dateTime:RFC3339',
}

type TypeTranslationMap = {[key in ExternalTypes]: InternalTypes}

const TO_COLUMN_TYPE: TypeTranslationMap = {
  [ExternalTypes.boolean]: InternalTypes.boolean,
  [ExternalTypes.unsignedLong]: InternalTypes.number,
  [ExternalTypes.long]: InternalTypes.number,
  [ExternalTypes.double]: InternalTypes.number,
  [ExternalTypes.string]: InternalTypes.string,
  [ExternalTypes.time]: InternalTypes.time,
}

type ParsedReturnTypes = string | undefined | null | number | boolean

function parseValue(
  value: string | undefined,
  columnType: InternalTypes
): ParsedReturnTypes {
  if (value === undefined) {
    return undefined
  }

  if (value === 'null') {
    return null
  }

  if (value === 'NaN') {
    return NaN
  }

  if (columnType === InternalTypes.boolean && value === 'true') {
    return true
  }

  if (columnType === InternalTypes.boolean && value === 'false') {
    return false
  }

  if (columnType === InternalTypes.string) {
    return value
  }

  if (columnType === InternalTypes.time) {
    return Date.parse(value)
  }

  if (columnType === InternalTypes.number && value === '') {
    return null
  }

  if (columnType === InternalTypes.number) {
    return Number(value)
  }

  return null
}

/*
    Convert a [Flux CSV response][0] to a `Table`.
    For example, given a series of Flux tables that look like this:
        column_a | column_b | column_c  <-- name
        long     | string   | long      <-- type
        ------------------------------
               1 |      "g" |       34
               2 |      "f" |       58
               3 |      "c" |       21
        column_b | column_d   <-- name
        double   | boolean    <-- type
        -------------------
             1.0 |     true
             2.0 |     true
             3.0 |     true

    This function will spread them out to a single wide table that looks like
    this instead:
        column_a | column_b (string) | column_c | column_b (number) | column_d  <-- key
        column_a | column_b          | column_c | column_b          | column_d  <-- name
        number   | string            | number   | number            | bool      <-- type
        ---------------------------------------------------------------------
               1 |               "g" |       34 |                   |
               2 |               "f" |       58 |                   |
               3 |                   |       21 |                   |
                 |                   |          |               1.0 |     true
                 |                   |          |               2.0 |     true
                 |                   |          |               3.0 |     true

    The `#group`, `#datatype`, and `#default` [annotations][1] are expected to be
    in the input Flux CSV.
    Values are coerced into appropriate JavaScript types based on the Flux
    `#datatype` annotation for the table
    The `Table` stores a `key` for each column which is separate from the column
    `name`. If multiple Flux tables have the same column but with different
    types, they will be distinguished by different keys in the resulting `Table`;
    otherwise the `key` and `name` for each column in the result table will be
    identical.

    [0]: https://github.com/influxdata/flux/blob/master/docs/SPEC.md#csv
    [1]: https://github.com/influxdata/flux/blob/master/docs/SPEC.md#annotations
    */

interface ChunkBoundary {
  start: number
  stop: number
}

type ParsedFluxColumnLookup = {[key: string]: ParsedFluxColumn}

export interface ParsedFluxColumn {
  name: string
  group: string
  type: InternalTypes
  default: string
  data: ParsedReturnTypes[]
}

export interface ParsedFlux {
  table: {
    columnKeys: string[]
    columns: ParsedFluxColumnLookup
    length: number
  }
  fluxGroupKeyUnion: string[]
}

export default function fromFlux(csv: string): ParsedFlux {
  /*
         A Flux CSV response can contain multiple CSV files each joined by a newline.
         See https://github.com/influxdata/flux/blob/master/docs/SPEC.md#multiple-tables.

         Split the response into separate chunks whenever we encounter:
             1. A newline
             2. Followed by any amount of whitespace
             3. Followed by a newline
             4. Followed by a `#` character
         The last condition is [necessary][0] for handling CSV responses with
         values containing newlines.

         [0]: https://github.com/influxdata/influxdb/issues/15017
     */
  const output: ParsedFluxColumnLookup = {},
    groupKey: {[key: string]: boolean} = {},
    names: {[key: string]: {[key: string]: boolean}} = {}

  const chunks: ChunkBoundary[] = [],
    regerz = /\n\s*\n#/g
  let match,
    lastRange = 0

  while ((match = regerz.exec(csv)) !== null) {
    chunks.push({
      start: lastRange,
      stop: match.index,
    })
    lastRange = match.index + match[0].length - 1
  }
  chunks.push({
    start: lastRange,
    stop: csv.length,
  })

  let runningTotal = 0,
    currentChunkIndex,
    headerLocation,
    currentLineInHeader,
    currentColumnIndex,
    currentLineIndex,
    columnName,
    columnType,
    columnKey,
    annotations,
    parsed

  for (
    currentChunkIndex = 0;
    currentChunkIndex < chunks.length;
    currentChunkIndex++
  ) {
    annotations = {}
    parsed = Papa.parse(
      csv.substring(
        chunks[currentChunkIndex].start,
        chunks[currentChunkIndex].stop
      ),
      {
        delimiter: ',',
        newline: '\n',
      }
    ).data

    // trim whitespace from the beginning
    while (parsed.length && parsed[0].length === 1) {
      parsed.shift()
    }

    // only happens on malformed input
    if (!parsed.length) {
      continue
    }

    headerLocation = 0
    while (/^\s*#/.test(parsed[headerLocation][0])) {
      headerLocation++
    }

    // only happens on malformed input
    if (parsed[headerLocation].length === 1) {
      continue
    }

    if (
      parsed[headerLocation][1] === 'error' &&
      parsed[headerLocation][2] === 'reference'
    ) {
      const errorReferenceCode = parsed[headerLocation + 1][2]
      const errorMessage = parsed[headerLocation + 1][1]

      throw new Error(`[${errorReferenceCode}] ${errorMessage}`)
    }

    // trim whitespace from the end
    while (parsed[parsed.length - 1].length < parsed[headerLocation].length) {
      parsed.pop()
    }

    for (
      currentLineInHeader = 0;
      currentLineInHeader < headerLocation;
      currentLineInHeader++
    ) {
      annotations[parsed[currentLineInHeader][0].trim()] = parsed[
        currentLineInHeader
      ].reduce((annotationObject, currentColumn, currentColumnIndex) => {
        const key = parsed[headerLocation][currentColumnIndex].trim()
        if (key) {
          annotationObject[key] = currentColumn.trim()
        }
        return annotationObject
      }, {})
    }

    for (
      currentColumnIndex = 1;
      currentColumnIndex < parsed[headerLocation].length;
      currentColumnIndex++
    ) {
      columnName = parsed[headerLocation][currentColumnIndex].trim()

      columnType = annotations['#datatype'][columnName]
      columnKey = `${columnName} (${TO_COLUMN_TYPE[columnType]})`

      if (!names.hasOwnProperty(columnName)) {
        names[columnName] = {}
      }

      if (!names[columnName].hasOwnProperty(columnKey)) {
        names[columnName][columnKey] = true
      }

      if (
        annotations['#group'] &&
        annotations['#group'].hasOwnProperty(columnName) &&
        annotations['#group'][columnName] === 'true'
      ) {
        groupKey[columnKey] = true
      }

      if (!output.hasOwnProperty(columnKey)) {
        output[columnKey] = {
          name: columnName,
          group: annotations['#group'][columnName],
          type: TO_COLUMN_TYPE[columnType],
          default: annotations['#default'][columnName],
          data: [],
        }
      }

      for (
        currentLineIndex = headerLocation + 1;
        currentLineIndex < parsed.length;
        currentLineIndex++
      ) {
        output[columnKey].data[
          runningTotal + currentLineIndex - headerLocation - 1
        ] = parseValue(
          parsed[currentLineIndex][currentColumnIndex] ||
            output[columnKey].default,
          output[columnKey].type
        )
      }
    }

    runningTotal += parsed.length - headerLocation - 1
  }

  /*
        Each column in a parsed `Table` can only have a single type, but because we
        combine columns from multiple Flux tables into a single table, we may
        encounter conflicting types for a given column during parsing.
        To avoid this issue, we separate the concept of the column _key_ and column
        _name_ in the `Table` object, where each key is unique but each name is not
        necessarily unique. We name the keys something like "foo (int)", where "foo"
        is the name and "int" is the type.
        But since type conflicts are rare and the public API requires referencing
        columns by key, we want to avoid unwieldy keys whenever possible. So the last
        stage of parsing is to rename all column keys from the `$NAME ($TYPE)` format
        to just `$NAME` if we can do so safely. That is what this function does.
    */
  Object.entries(names)
    .map(([k, v]) => {
      const columnNames = Object.keys(v)

      columnNames.forEach(n => {
        output[n].data.length = runningTotal
      })

      return [k, columnNames]
    })
    .filter(([_, v]) => v.length === 1)
    .map(([k, v]) => [k, v[0]])
    .forEach(e => {
      const k = e[0] as string
      const v = e[1] as string

      // TODO to reduce the cost of dumping this on the GC,
      // we should keep parsed results as an array, with output
      // keeping a pointer to it's index
      output[k] = output[v]
      delete output[v]

      if (groupKey.hasOwnProperty(v)) {
        groupKey[k] = true
        delete groupKey[v]
      }
    })

  return {
    table: {
      columnKeys: Object.keys(output),
      columns: output,
      length: runningTotal,
    },
    fluxGroupKeyUnion: Object.keys(groupKey),
  }
}
