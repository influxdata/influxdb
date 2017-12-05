import React, {PropTypes} from 'react'

import FancyScrollbar from 'shared/components/FancyScrollbar'
import LogsTableRow from 'src/kapacitor/components/LogsTableRow'

const LogsTable = ({logs}) =>
  <div className="logs-table--container">
    <div className="logs-table--header">
      <h2 className="panel-title">Logs</h2>
    </div>
    <FancyScrollbar
      className="logs-table--panel fancy-scroll--kapacitor"
      autoHide={false}
    >
      <div className="logs-table">
        {logs.length
          ? logs.map((log, i) =>
              <LogsTableRow key={log.key} logItem={log} index={i} />
            )
          : <div className="page-spinner" />}
      </div>
    </FancyScrollbar>
  </div>

const {arrayOf, shape, string} = PropTypes

LogsTable.propTypes = {
  logs: arrayOf(
    shape({
      key: string.isRequired,
      ts: string.isRequired,
      lvl: string.isRequired,
      msg: string.isRequired,
    })
  ).isRequired,
}

export default LogsTable
