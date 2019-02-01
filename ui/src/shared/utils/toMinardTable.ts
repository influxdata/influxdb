import {FluxTable} from 'src/types'
import {Table, ColumnType} from 'src/minard'

export const SCHEMA_ERROR_MESSAGE = 'Encountered'

interface ToMinardTableResult {
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
  - Values are coerced into approriate JavaScript types based on the Flux
    `#datatype` annotation for the table
  - If a resulting column has data of conflicting types, only the values for
    the first data type encountered are kept

*/
export const toMinardTable = (tables: FluxTable[]): ToMinardTableResult => {
  const columns = {}
  const columnTypes = {}
  const schemaConflicts = []

  let k = 0

  for (const table of tables) {
    const header = table.data[0]

    if (!header) {
      // Ignore empty tables
      continue
    }

    for (let j = 0; j < header.length; j++) {
      const column = header[j]

      let columnConflictsSchema = false

      if (column === '' || column === 'result') {
        // Ignore these columns
        continue
      }

      const columnType = toMinardColumnType(table.dataTypes[column])

      if (columnTypes[column] && columnTypes[column] !== columnType) {
        schemaConflicts.push(column)
        columnConflictsSchema = true
      } else if (!columnTypes[column]) {
        columns[column] = []
        columnTypes[column] = columnType
      }

      for (let i = 1; i < table.data.length; i++) {
        // TODO: Refactor to treat each column as a `(name, dataType)` tuple
        // rather than just a `name`. This will let us avoid dropping data due
        // to schema conflicts
        const value = columnConflictsSchema
          ? undefined
          : parseValue(table.data[i][j].trim(), columnType)

        columns[column][k + i - 1] = value
      }
    }

    k += table.data.length - 1
  }

  const result: ToMinardTableResult = {
    table: {columns, columnTypes},
    schemaConflicts,
  }

  return result
}

const TO_MINARD_COLUMN_TYPE = {
  boolean: ColumnType.Boolean,
  unsignedLong: ColumnType.Numeric,
  long: ColumnType.Numeric,
  double: ColumnType.Numeric,
  string: ColumnType.Categorical,
  'dateTime:RFC3339': ColumnType.Temporal,
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

  if (columnType === ColumnType.Boolean && value === 'true') {
    return true
  }

  if (columnType === ColumnType.Boolean && value === 'false') {
    return false
  }

  if (columnType === ColumnType.Categorical) {
    return value
  }

  if (columnType === ColumnType.Numeric) {
    return Number(value)
  }

  if (columnType === ColumnType.Temporal) {
    return Date.parse(value)
  }

  return null
}
