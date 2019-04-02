import {FluxTable} from 'src/types'
import {Table, ColumnType, isNumeric} from '@influxdata/vis'

export const GROUP_KEY_COL_NAME = 'group_key'

export interface ToMinardTableResult {
  table: Table
  schemaConflicts: string[]
}

/*
  Convert a series of `FluxTable`s to the table format used by the _Minard_
  visualization library.

  For example, given a series of Flux tables that look like this:

      column_a | column_b | column_c
      ------------------------------
             1 |      "g" |       34
             2 |      "f" |       58 
             3 |      "c" |       21

      column_b | column_d 
      --------------------
           "h" |      true
           "g" |      true
           "c" |      true
  
  This function will spread them out to a single wide table that looks like
  this instead:

      column_a | column_b | column_c | column_d
      -----------------------------------------
             1 |      "g" |       34 | 
             2 |      "f" |       58 |
             3 |      "c" |       21 |
               |      "h" |          |     true
               |      "g" |          |     true
               |      "c" |          |     true


  Note that:

  - If a value doesn't exist for a column, it is `undefined` in the result
  - If a value does exist for a column but was specified as `null` in the Flux
    response, it will be `null` in the result
  - Values are coerced into appropriate JavaScript types based on the Flux
    `#datatype` annotation for the table
  - If a resulting column has data of conflicting types, only the values for
    the first data type encountered are kept
  - The "table" column of each table is handled specially, as it represents the
    group key for the table

*/
export const toMinardTable = (tables: FluxTable[]): ToMinardTableResult => {
  const outColumns = {}
  const schemaConflicts = []

  let k = 0

  for (const table of tables) {
    const header = table.data[0]

    if (!header) {
      // Ignore empty tables
      continue
    }

    for (let j = 0; j < header.length; j++) {
      const columnName = header[j]

      if (columnName === '' || columnName === 'table') {
        // Ignore these columns
        continue
      }

      const columnType = toMinardColumnType(table.dataTypes[columnName])

      let columnConflictsSchema = false

      if (
        outColumns[columnName] &&
        outColumns[columnName].type !== columnType
      ) {
        schemaConflicts.push(columnName)
        columnConflictsSchema = true
      } else if (!outColumns[columnName]) {
        outColumns[columnName] = {data: [], type: columnType}
      }

      for (let i = 1; i < table.data.length; i++) {
        let value

        if (columnName === 'result') {
          value = table.result
        } else if (!columnConflictsSchema) {
          value = parseValue(table.data[i][j].trim(), columnType)
        }

        outColumns[columnName].data[k + i - 1] = value
      }
    }

    k += table.data.length - 1
  }

  const result: ToMinardTableResult = {
    table: {columns: outColumns, length: k},
    schemaConflicts,
  }

  return result
}

const TO_MINARD_COLUMN_TYPE = {
  boolean: 'bool',
  unsignedLong: 'uint',
  long: 'int',
  double: 'float',
  string: 'string',
  'dateTime:RFC3339': 'time',
}

const toMinardColumnType = (fluxDataType: string): ColumnType => {
  const columnType = TO_MINARD_COLUMN_TYPE[fluxDataType]

  if (!columnType) {
    throw new Error(`encountered unknown Flux column type ${fluxDataType}`)
  }

  return columnType
}

const parseValue = (value: string, columnType: ColumnType): any => {
  if (value === 'null') {
    return null
  }

  if (value === 'NaN') {
    return NaN
  }

  if (columnType === 'bool' && value === 'true') {
    return true
  }

  if (columnType === 'bool' && value === 'false') {
    return false
  }

  if (columnType === 'string') {
    return value
  }

  if (columnType === 'time') {
    return Date.parse(value)
  }

  if (isNumeric(columnType)) {
    return Number(value)
  }

  return null
}
