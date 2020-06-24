// Utils
import {runQuery} from 'src/shared/apis/query'
import {fromFlux} from '@influxdata/giraffe'
import {event} from 'src/cloud/utils/reporting'

// Constants
import {MONITORING_BUCKET} from 'src/alerting/constants'

// Types
import {CancelBox, StatusRow, File} from 'src/types'
import {RunQueryResult} from 'src/shared/apis/query'
import {Row} from 'src/eventViewer/types'

export const runStatusesQuery = (
  orgID: string,
  checkID: string,
  extern: File
): CancelBox<StatusRow[][]> => {
  const query = `
from(bucket: "${MONITORING_BUCKET}")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "statuses" and r._field == "_message")
  |> filter(fn: (r) => r._check_id == "${checkID}")
  |> filter(fn: (r) => exists r._value and exists r._check_id and exists r._check_name and exists r._level)
  |> keep(columns: ["_time", "_value", "_check_id", "_check_name", "_level"])
  |> window(every: 1s, timeColumn: "_time", startColumn: "_start", stopColumn: "_stop")
  |> group(columns: ["_start", "_stop"])
  |> rename(columns: {"_time": "time",
                      "_value": "message",
                      "_check_id": "checkID",
                      "_check_name": "checkName",
                      "_level": "level"})
`

  event('runQuery', {context: 'checkStatuses'})
  return processStatusesResponse(runQuery(orgID, query, extern)) as CancelBox<
    StatusRow[][]
  >
}

/*
  Convert a Flux CSV response that is grouped into tables into a list of objects.
*/
export const processStatusesResponse = ({
  promise: queryPromise,
  cancel,
}: CancelBox<RunQueryResult>): CancelBox<Row[][]> => {
  const promise = queryPromise.then<Row[][]>(resp => {
    if (resp.type !== 'SUCCESS') {
      return Promise.reject(new Error(resp.message))
    }

    const {table} = fromFlux(resp.csv)
    const rows: Row[][] = [[]]

    for (let i = 0; i < table.length; i++) {
      const row = {}

      for (const key of table.columnKeys) {
        row[key] = table.getColumn(key)[i]
      }

      const tableIndex = row['table']

      if (!rows[tableIndex]) {
        rows[tableIndex] = [row]
      } else {
        rows[tableIndex].push(row)
      }
    }

    return rows
  })

  return {
    promise,
    cancel,
  }
}
