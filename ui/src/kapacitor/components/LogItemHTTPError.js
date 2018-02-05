import React, {PropTypes} from 'react'

const LogItemHTTPError = ({logItem}) =>
  <div className="logs-table--row" key={logItem.key}>
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">
        {logItem.ts}
      </div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--service error">HTTP Server</div>
      <div className="logs-table--columns">
        <div className="logs-table--key-values error">
          ERROR: {logItem.msg}
        </div>
      </div>
    </div>
  </div>

const {shape, string} = PropTypes

LogItemHTTPError.propTypes = {
  logItem: shape({
    key: string.isRequired,
    lvl: string.isRequired,
    ts: string.isRequired,
    msg: string.isRequired,
  }),
}

export default LogItemHTTPError
