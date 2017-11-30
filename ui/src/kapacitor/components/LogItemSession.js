import React, {PropTypes} from 'react'

const LogItemSession = ({logItem}) =>
  <div className="logs-table--row">
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">
        {logItem.ts}
      </div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--session">
        {logItem.msg}
      </div>
    </div>
  </div>

const {shape, string} = PropTypes

LogItemSession.propTypes = {
  logItem: shape({
    lvl: string.isRequired,
    ts: string.isRequired,
    msg: string.isRequired,
  }),
}

export default LogItemSession
