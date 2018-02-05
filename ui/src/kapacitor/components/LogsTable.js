import React, {PropTypes} from 'react'

import LogsTableRow from 'src/kapacitor/components/LogsTableRow'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'

const numLogsToRender = 200

const LogsTable = ({logs, onToggleExpandLog}) =>
  <div className="logs-table--container">
    <div className="logs-table--header">
      <h2 className="panel-title">{`${numLogsToRender} Most Recent Logs`}</h2>
    </div>
    <FancyScrollbar
      autoHide={false}
      className="logs-table--panel fancy-scroll--kapacitor"
    >
      {logs
        .slice(0, numLogsToRender)
        .map((log, i) =>
          <LogsTableRow
            key={log.key}
            logItem={log}
            index={i}
            onToggleExpandLog={onToggleExpandLog}
          />
        )}
    </FancyScrollbar>
  </div>

const {arrayOf, func, shape, string} = PropTypes

LogsTable.propTypes = {
  logs: arrayOf(
    shape({
      key: string.isRequired,
      ts: string.isRequired,
      lvl: string.isRequired,
      msg: string.isRequired,
    })
  ).isRequired,
  onToggleExpandLog: func.isRequired,
}

export default LogsTable
