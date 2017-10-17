import React, {Component, PropTypes} from 'react'

import {DUMMY_LOGS} from 'src/kapacitor/constants/dummyLogs'

class LogsTable extends Component {
  constructor(props) {
    super(props)
  }

  renderAlertLevel = level => {
    let alertCSS
    switch (level) {
      case 'ok':
        alertCSS = 'label label-success'
        break
      case 'warn':
        alertCSS = 'label label-info'
        break
      case 'error':
        alertCSS = 'label label-danger'
        break
      case 'debug':
        alertCSS = 'label label-primary'
        break
      default:
        alertCSS = 'label label-default'
    }
    return alertCSS
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

  renderEmptyCell = () => {
    return <span className="logs-table--empty-cell">--</span>
  }
  renderMessage = log => {
    if (log.msg === 'http request') {
      return `HTTP Request ${log.username}@${log.host}`
    }
    return log.msg
  }

  renderTable = () => {
    return (
      <table className="table table-highlight logs-table">
        <thead>
          <tr>
            {/* <th>Timestamp</th> */}
            <th>Service</th>
            <th>Level</th>
            <th>Task</th>
            <th>Node</th>
            <th>Duration</th>
            <th>Message</th>
            <th>Tags & Fields</th>
          </tr>
        </thead>
        <tbody>
          {DUMMY_LOGS.map((l, i) =>
            <tr key={i}>
              {/* <td>
                {l.ts}
              </td> */}
              <td>
                {l.service}
              </td>
              <td>
                <span className={this.renderAlertLevel(l.lvl)}>
                  {l.lvl}
                </span>
              </td>
              <td>
                {l.task || this.renderEmptyCell()}
              </td>
              <td>
                {l.node || this.renderEmptyCell()}
              </td>
              <td>
                {l.duration || this.renderEmptyCell()}
              </td>
              <td>
                {this.renderMessage(l)}
              </td>
              <td>
                {l.tag ? <div>TAGS</div> : null}
                {this.renderKeysAndValues(l.tag)}
                {l.field ? <div>FIELDS</div> : null}
                {this.renderKeysAndValues(l.field)}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    )
  }
  render() {
    const {isWidget} = this.props

    const output = isWidget
      ? this.renderTable()
      : <div className="logs-table--container">
          <div className="logs-table--header">
            <h2 className="panel-title">Logs</h2>
            <div className="filterthing">FILTER</div>
          </div>
          <div className="logs-table--panel">
            {this.renderTable()}
          </div>
        </div>

    return output
  }
}

const {bool} = PropTypes

LogsTable.propTypes = {
  isWidget: bool,
}

export default LogsTable
