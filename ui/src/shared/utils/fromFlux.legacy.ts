import fromFlux from 'src/shared/utils/fromFlux'
import {newTable, Table} from '@influxdata/giraffe'

/*\

  This module translates interfaces between giraffe and influxdb
  as we migrate the flux parsing over from the vis library, closer
  to the api response layer

\*/
export interface FromFluxResult {
  // The single parsed `Table`
  table: Table

  // The union of unique group keys from all input Flux tables
  fluxGroupKeyUnion: string[]
}

export default function fromFluxLegacy(csv: string): FromFluxResult {
  const parsedFlux = fromFlux(csv)

  return {
    table: Object.entries(parsedFlux.table.columns).reduce(
      (table, [key, column]) => {
        return table.addColumn(
          key,
          column.type,
          column.data as string[],
          column.name
        )
      },
      newTable(parsedFlux.table.length)
    ),
    fluxGroupKeyUnion: parsedFlux.fluxGroupKeyUnion,
  }
}
