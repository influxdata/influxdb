// Utils
import {runQuery} from 'src/shared/apis/query'
import {processResponse} from 'src/alerting/utils/history'

// Constants
import {MONITORING_BUCKET} from 'src/alerting/constants/history'

// Types
import {CancelBox, StatusRow, File} from 'src/types'

export const runStatusesQuery = (
  orgID: string,
  checkID: string,
  extern: File
): CancelBox<StatusRow[]> => {
  const query = `
from(bucket: "${MONITORING_BUCKET}")
  |> range(start: v.timeRangeStart, stop: v.timeRangeStop)
  |> filter(fn: (r) => r._measurement == "statuses" and r._field == "_message")
  |> keep(columns: ["_time", "_value", "_check_id", "_check_name", "_level"])
  |> rename(columns: {"_time": "time",
                      "_value": "message",
                      "_check_id": "checkID",
                      "_check_name": "checkName",
                      "_level": "level"})
  |> group()
  |> filter(fn: (r) => r["checkID"] == "${checkID}")
  |> sort(columns: ["time"], desc: true)
`
  return processResponse(runQuery(orgID, query, extern)) as CancelBox<
    StatusRow[]
  >
}
