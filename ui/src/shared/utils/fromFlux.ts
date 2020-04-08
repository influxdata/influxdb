import Papa from 'papaparse'

const TO_COLUMN_TYPE = {
  boolean: 'boolean',
  unsignedLong: 'number',
  long: 'number',
  double: 'number',
  string: 'string',
  'dateTime:RFC3339': 'time',
}

function parseValue(value, columnType) {
  if (value === undefined) {
    return undefined
  }

  if (value === 'null') {
    return null
  }

  if (value === 'NaN') {
    return NaN
  }

  if (columnType === 'boolean' && value === 'true') {
    return true
  }

  if (columnType === 'boolean' && value === 'false') {
    return false
  }

  if (columnType === 'string') {
    return value
  }

  if (columnType === 'time') {
    return Date.parse(value)
  }

  if (columnType === 'number' && value === '') {
    return null
  }

  if (columnType === 'number') {
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
    The `Table` stores a `key` for each column which is seperate from the column
    `name`. If multiple Flux tables have the same column but with different
    types, they will be distinguished by different keys in the resulting `Table`;
    otherwise the `key` and `name` for each column in the result table will be
    identical.

    [0]: https://github.com/influxdata/flux/blob/master/docs/SPEC.md#csv
    [1]: https://github.com/influxdata/flux/blob/master/docs/SPEC.md#annotations
    */

export default function fromFlux(csv) {
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
  const output = {},
    groupKey = {},
    names = {}

  const chunks = [],
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
    ni,
    headerLocation,
    no,
    na,
    colName,
    colType,
    colKey,
    annotations,
    parsed

  for (ni = 0; ni < chunks.length; ni++) {
    annotations = {}
    parsed = Papa.parse(csv.substring(chunks[ni].start, chunks[ni].stop)).data

    headerLocation = 0
    while (/^\s*#/.test(parsed[headerLocation][0])) {
      headerLocation++
    }

    if (
      parsed[headerLocation][1] === 'error' &&
      parsed[headerLocation][2] === 'reference'
    ) {
      const ref = parsed[headerLocation + 1][2]
      const msg = parsed[headerLocation + 1][1]

      throw new Error(`[${ref}] ${msg}`)
    }

    for (no = 0; no < headerLocation; no++) {
      annotations[parsed[no][0]] = parsed[no].reduce((p, c, i) => {
        p[parsed[headerLocation][i]] = c
        return p
      }, {})
    }

    for (no = 1; no < parsed[headerLocation].length; no++) {
      colName = parsed[headerLocation][no]

      colType = annotations['#datatype'][colName]
      colKey = `${colName} (${TO_COLUMN_TYPE[colType]})`

      if (!names.hasOwnProperty(colName)) {
        names[colName] = {}
      }

      if (!names[colName].hasOwnProperty(colKey)) {
        names[colName][colKey] = true
      }

      if (
        annotations['#group'] &&
        annotations['#group'].hasOwnProperty(colName) &&
        annotations['#group'][colName] === 'true'
      ) {
        groupKey[colKey] = true
      }

      if (!output.hasOwnProperty(colKey)) {
        output[colKey] = {
          name: colName,
          group: annotations['#group'][colName],
          type: TO_COLUMN_TYPE[colType],
          default: annotations['#default'][colName],
          data: [],
        }
      }

      for (na = headerLocation + 1; na < parsed.length; na++) {
        output[colKey].data[
          runningTotal + na - headerLocation - 1
        ] = parseValue(
          parsed[na][no] || output[colKey].default,
          output[colKey].type
        )
      }
    }

    runningTotal += parsed.length - headerLocation - 1
  }

  /*
        Each column in a parsed `Table` can only have a single type, but because we
        combine columns from multiple Flux tables into a single table, we may
        encounter conflicting types for a given column during parsing.
        To avoid this issue, we seperate the concept of the column _key_ and column
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
      const colNames = Object.keys(v)

      colNames.forEach(n => {
        output[n].data.length = runningTotal
      })

      return [k, colNames]
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
