import React, {PropTypes} from 'react'

const LogItemHTTP = ({logItem}) =>
  <div className="logs-table--row">
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">
        {logItem.ts}
      </div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--service">HTTP Request</div>
      <div className="logs-table--http">
        {logItem.method} {logItem.username}@{logItem.host} ({logItem.duration})
      </div>
    </div>
  </div>

const {shape, string} = PropTypes

LogItemHTTP.propTypes = {
  logItem: shape({
    lvl: string.isRequired,
    ts: string.isRequired,
    method: string.isRequired,
    username: string.isRequired,
    host: string.isRequired,
    duration: string.isRequired,
  }),
}

export default LogItemHTTP
