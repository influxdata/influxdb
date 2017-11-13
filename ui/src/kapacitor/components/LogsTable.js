import React, {Component, PropTypes} from 'react'

import LogItemSession from 'src/kapacitor/components/LogItemSession'
import LogItemHTTP from 'src/kapacitor/components/LogItemHTTP'
import LogItemHTTPError from 'src/kapacitor/components/LogItemHTTPError'
import LogItemKapacitorPoint from 'src/kapacitor/components/LogItemKapacitorPoint'
import LogItemKapacitorError from 'src/kapacitor/components/LogItemKapacitorError'
import LogItemKapacitorDebug from 'src/kapacitor/components/LogItemKapacitorDebug'
import LogItemInfluxDBDebug from 'src/kapacitor/components/LogItemInfluxDBDebug'

class LogsTable extends Component {
  constructor(props) {
    super(props)
  }

  renderTableRow = (logItem, index) => {
    if (logItem.service === 'sessions') {
      return <LogItemSession logItem={logItem} key={index} />
    }
    if (logItem.service === 'http' && logItem.msg === 'http request') {
      return <LogItemHTTP logItem={logItem} key={index} />
    }
    if (logItem.service === 'kapacitor' && logItem.msg === 'point') {
      return <LogItemKapacitorPoint logItem={logItem} key={index} />
    }
    if (logItem.service === 'httpd_server_errors' && logItem.lvl === 'error') {
      return <LogItemHTTPError logItem={logItem} key={index} />
    }
    if (logItem.service === 'kapacitor' && logItem.lvl === 'error') {
      return <LogItemKapacitorError logItem={logItem} key={index} />
    }
    if (logItem.service === 'kapacitor' && logItem.lvl === 'debug') {
      return <LogItemKapacitorDebug logItem={logItem} key={index} />
    }
    if (logItem.service === 'influxdb' && logItem.lvl === 'debug') {
      return <LogItemInfluxDBDebug logItem={logItem} key={index} />
    }

    return (
      <div className="logs-table--row" key={index}>
        <div className="logs-table--divider">
          <div className={`logs-table--level ${logItem.lvl}`} />
          <div className="logs-table--timestamp">
            {logItem.ts}
          </div>
        </div>
        <div className="logs-table--details">
          <div className="logs-table--service debug">Thing</div>
          <div className="logs-table--blah">
            <div className="logs-table--key-values">
              <p>Thang</p>
            </div>
          </div>
        </div>
      </div>
    )
  }

  render() {
    const {logs} = this.props

    return (
      <div className="logs-table--container">
        <div className="logs-table--header">
          <h2 className="panel-title">Logs</h2>
          <div className="filterthing">
            <input
              type="text"
              className="form-control input-sm"
              placeholder="Search Logs..."
            />
          </div>
        </div>
        <div className="logs-table--panel">
          <div className="logs-table">
            {logs.length
              ? logs.map((log, i) => this.renderTableRow(log, i))
              : <div className="page-spinner" />}
          </div>
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, shape, string} = PropTypes

LogsTable.propTypes = {
  isWidget: bool,
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
