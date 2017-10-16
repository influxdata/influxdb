import React, {Component, PropTypes} from 'react'

class LogsTable extends Component {
  constructor(props) {
    super(props)
  }

  renderTable = () => {
    return (
      <table className="table logs-table">
        <thead>
          <tr>
            <th>Blargh</th>
            <th>Swoggle</th>
            <th>Horgles</th>
            <th>Chortle</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>sdfsdfsf</td>
            <td>sdfsdfsdf</td>
            <td>sdfsdfsdfjnsdfkjlnjsdjkfsf</td>
            <td>sdbnds</td>
          </tr>
          <tr>
            <td>sdfsdfsf</td>
            <td>sdfsdfsdf</td>
            <td>sdfsdfsdfjnsdfkjlnjsdjkfsf</td>
            <td>sdbnds</td>
          </tr>
        </tbody>
      </table>
    )
  }
  render() {
    const {isWidget} = this.props

    const output = isWidget
      ? this.renderTable()
      : <div className="container-fluid logs-table-container">
          <div className="row">
            <div className="col-xs-12">
              <div className="panel panel-minimal">
                <div className="panel-heading u-flex u-ai-center u-jc-space-between">
                  <h2 className="panel-title">Logs</h2>
                  <div className="filterthing">FILTER</div>
                </div>
                <div className="panel-body">
                  {this.renderTable()}
                </div>
              </div>
            </div>
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
