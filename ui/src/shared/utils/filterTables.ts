import {FluxTable} from 'src/types'

const IGNORED_COLUMNS = ['', 'result', 'table', '_start', '_stop']

export const filterTables = (tables: FluxTable[]): FluxTable[] => {
  return tables.map(table => {
    const header = table.data[0]
    const indices = IGNORED_COLUMNS.map(name => header.indexOf(name))

    const tableData = table.data

    const data = tableData.map(row => {
      return row.filter((__, i) => !indices.includes(i))
    })

    return {
      ...table,
      data,
    }
  })
}
