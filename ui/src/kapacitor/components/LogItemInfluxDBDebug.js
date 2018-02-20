import React, {PropTypes} from 'react'

const LogItemInfluxDBDebug = ({logItem}) =>
  <div className="logs-table--row">
    <div className="logs-table--divider">
      <div className={`logs-table--level ${logItem.lvl}`} />
      <div className="logs-table--timestamp">
        {logItem.ts}
      </div>
    </div>
    <div className="logs-table--details">
      <div className="logs-table--service debug">InfluxDB</div>
      <div className="logs-table--columns">
        <div className="logs-table--key-values debug">
          DEBUG: {logItem.msg}
          <br />
          Cluster: {logItem.cluster}
        </div>
      </div>
    </div>
  </div>

const {shape, string} = PropTypes

LogItemInfluxDBDebug.propTypes = {
  logItem: shape({
    lvl: string.isRequired,
    ts: string.isRequired,
    msg: string.isRequired,
    cluster: string.isRequired,
  }),
}

export default LogItemInfluxDBDebug
