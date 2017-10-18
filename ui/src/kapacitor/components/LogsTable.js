import React, {Component, PropTypes} from 'react'

import {DUMMY_LOGS} from 'src/kapacitor/constants/dummyLogs'

class LogsTable extends Component {
  constructor(props) {
    super(props)
  }

  renderKeysAndValues = object => {
    if (!object) {
      return <span className="logs-table--empty-cell">--</span>
    }
    const objKeys = Object.keys(object)
    const objValues = Object.values(object)

    const objElements = objKeys.map((objKey, i) =>
      <div key={i} className="logs-table--key-value">
        {objKey}: <span>{objValues[i]}</span>
      </div>
    )
    return objElements
  }

  renderTableRow = (logItem, i) => {
    let rowDetails

    if (logItem.service === 'sessions') {
      rowDetails = (
        <div className="logs-table--details">
          <div className="logs-table--session">
            {logItem.msg}
          </div>
        </div>
      )
    }
    if (logItem.service === 'http' && logItem.msg === 'http request') {
      rowDetails = (
        <div className="logs-table--details">
          <div className="logs-table--service">HTTP Request</div>
          <div className="logs-table--http">
            {logItem.method} {logItem.username}@{logItem.host} ({logItem.duration})
          </div>
        </div>
      )
    }
    if (logItem.service === 'kapacitor' && logItem.msg === 'point') {
      rowDetails = (
        <div className="logs-table--details">
          <div className="logs-table--service">Kapacitor Point</div>
          <div className="logs-table--blah">
            <div className="logs-table--key-values">
              TAGS<br />
              {this.renderKeysAndValues(logItem.tag)}
            </div>
            <div className="logs-table--key-values">
              FIELDS<br />
              {this.renderKeysAndValues(logItem.field)}
            </div>
          </div>
        </div>
      )
    }
    if (logItem.service === 'httpd_server_errors' && logItem.lvl === 'error') {
      rowDetails = (
        <div className="logs-table--details">
          <div className="logs-table--service error">HTTP Server</div>
          <div className="logs-table--blah">
            <div className="logs-table--key-values error">
              ERROR: {logItem.msg}
            </div>
          </div>
        </div>
      )
    }
    if (logItem.service === 'kapacitor' && logItem.lvl === 'error') {
      rowDetails = (
        <div className="logs-table--details">
          <div className="logs-table--service error">Kapacitor</div>
          <div className="logs-table--blah">
            <div className="logs-table--key-values error">
              ERROR: {logItem.msg}
            </div>
          </div>
        </div>
      )
    }
    if (logItem.service === 'kapacitor' && logItem.lvl === 'debug') {
      rowDetails = (
        <div className="logs-table--details">
          <div className="logs-table--service debug">Kapacitor</div>
          <div className="logs-table--blah">
            <div className="logs-table--key-values debug">
              DEBUG: {logItem.msg}
            </div>
          </div>
        </div>
      )
    }
    if (logItem.service === 'influxdb' && logItem.lvl === 'debug') {
      rowDetails = (
        <div className="logs-table--details">
          <div className="logs-table--service debug">InfluxDB</div>
          <div className="logs-table--blah">
            <div className="logs-table--key-values debug">
              DEBUG: {logItem.msg}
              <br />
              Cluster: {logItem.cluster}
            </div>
          </div>
        </div>
      )
    }

    return (
      <div className="logs-table--row" key={i}>
        <div className="logs-table--divider">
          <div className={`logs-table--level ${logItem.lvl}`} />
          <div className="logs-table--timestamp">
            {logItem.ts}
          </div>
        </div>
        {rowDetails}
      </div>
    )
  }

  render() {
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
            {DUMMY_LOGS.map((l, i) => this.renderTableRow(l, i))}
          </div>
        </div>
      </div>
    )
  }
}

const {bool} = PropTypes

LogsTable.propTypes = {
  isWidget: bool,
}

export default LogsTable
