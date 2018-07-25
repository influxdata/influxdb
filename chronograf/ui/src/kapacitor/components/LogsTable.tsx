import React, {SFC} from 'react'

import LogsTableRow from 'src/kapacitor/components/LogsTableRow'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

import {LogItem} from 'src/types/kapacitor'

const numLogsToRender = 200

interface Props {
  logs: LogItem[]
}

const LogsTable: SFC<Props> = ({logs}) => (
  <div className="logs-table">
    <div className="logs-table--header">
      {`${numLogsToRender} Most Recent Logs`}
    </div>
    <FancyScrollbar
      autoHide={false}
      className="logs-table--container fancy-scroll--kapacitor"
    >
      {logs
        .slice(0, numLogsToRender)
        .map(log => <LogsTableRow key={log.key} logItem={log} />)}
    </FancyScrollbar>
  </div>
)

export default LogsTable
