import React, {Component, PropTypes} from 'react'
import _ from 'lodash'
import {Link} from 'react-router'

class AlertsTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
      filteredAlerts: this.props.alerts,
      sortDirection: null,
      sortKey: null,
    }

    this.filterAlerts = ::this.filterAlerts
    this.changeSort = ::this.changeSort
    this.sortableClasses = ::this.sortableClasses
    this.sort = ::this.sort
  }

  componentWillReceiveProps(newProps) {
    this.filterAlerts(this.state.searchTerm, newProps.alerts)
  }

  filterAlerts(searchTerm, newAlerts) {
    const alerts = newAlerts || this.props.alerts
    const filterText = searchTerm.toLowerCase()
    const filteredAlerts = alerts.filter(h => {
      if (h.host === null || h.name === null || h.level === null) {
        return false
      }

      return (
        h.name.toLowerCase().includes(filterText) ||
        h.host.toLowerCase().includes(filterText) ||
        h.level.toLowerCase().includes(filterText)
      )
    })
    this.setState({searchTerm, filteredAlerts})
  }

  changeSort(key) {
    // if we're using the key, reverse order; otherwise, set it with ascending
    if (this.state.sortKey === key) {
      const reverseDirection = this.state.sortDirection === 'asc'
        ? 'desc'
        : 'asc'
      this.setState({sortDirection: reverseDirection})
    } else {
      this.setState({sortKey: key, sortDirection: 'asc'})
    }
  }

  sortableClasses(key) {
    if (this.state.sortKey === key) {
      if (this.state.sortDirection === 'asc') {
        return 'sortable-header sorting-ascending'
      }
      return 'sortable-header sorting-descending'
    }
    return 'sortable-header'
  }

  sort(alerts, key, direction) {
    switch (direction) {
      case 'asc':
        return _.sortBy(alerts, e => e[key])
      case 'desc':
        return _.sortBy(alerts, e => e[key]).reverse()
      default:
        return alerts
    }
  }

  renderTable() {
    const {source: {id}} = this.props
    const alerts = this.sort(
      this.state.filteredAlerts,
      this.state.sortKey,
      this.state.sortDirection
    )
    return this.props.alerts.length
      ? <table className="table v-center table-highlight">
          <thead>
            <tr>
              <th
                onClick={() => this.changeSort('name')}
                className={this.sortableClasses('name')}
              >
                Name
              </th>
              <th
                onClick={() => this.changeSort('level')}
                className={this.sortableClasses('level')}
              >
                Level
              </th>
              <th
                onClick={() => this.changeSort('time')}
                className={this.sortableClasses('time')}
              >
                Time
              </th>
              <th
                onClick={() => this.changeSort('host')}
                className={this.sortableClasses('host')}
              >
                Host
              </th>
              <th
                onClick={() => this.changeSort('value')}
                className={this.sortableClasses('value')}
              >
                Value
              </th>
            </tr>
          </thead>
          <tbody>
            {alerts.map(({name, level, time, host, value}) => {
              return (
                <tr key={`${name}-${level}-${time}-${host}-${value}`}>
                  <td className="monotype">{name}</td>
                  <td className={`monotype alert-level-${level.toLowerCase()}`}>
                    {level}
                  </td>
                  <td className="monotype">
                    {new Date(Number(time)).toISOString()}
                  </td>
                  <td className="monotype">
                    <Link to={`/sources/${id}/hosts/${host}`}>
                      {host}
                    </Link>
                  </td>
                  <td className="monotype">{value}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      : this.renderTableEmpty()
  }

  renderTableEmpty() {
    const {source: {id}, shouldNotBeFilterable} = this.props

    return shouldNotBeFilterable
      ? <div className="graph-empty">
          <p>
            Learn how to configure your first <strong>Rule</strong> in<br />
            the <em>Getting Started</em> guide
          </p>
        </div>
      : <div className="generic-empty-state">
          <h4 className="no-user-select">
            There are no Alerts to display
          </h4>
          <br />
          <h6 className="no-user-select">
            Try changing the Time Range or
            <Link
              style={{marginLeft: '10px'}}
              to={`/sources/${id}/alert-rules/new`}
              className="btn btn-primary btn-sm"
            >
              Create an Alert Rule
            </Link>
          </h6>
        </div>
  }

  render() {
    const {
      shouldNotBeFilterable,
      limit,
      onGetMoreAlerts,
      isAlertsMaxedOut,
      alertsCount,
    } = this.props

    return shouldNotBeFilterable
      ? <div className="alerts-widget">
          {this.renderTable()}
          {limit && alertsCount
            ? <button
                className="btn btn-sm btn-default btn-block"
                onClick={onGetMoreAlerts}
                disabled={isAlertsMaxedOut}
                style={{marginBottom: '20px'}}
              >
                {isAlertsMaxedOut
                  ? `All ${alertsCount} Alerts displayed`
                  : 'Load next 30 Alerts'}
              </button>
            : null}
        </div>
      : <div className="panel panel-minimal">
          <div className="panel-heading u-flex u-ai-center u-jc-space-between">
            <h2 className="panel-title">{this.props.alerts.length} Alerts</h2>
            {this.props.alerts.length
              ? <SearchBar onSearch={this.filterAlerts} />
              : null}
          </div>
          <div className="panel-body">
            {this.renderTable()}
          </div>
        </div>
  }
}

class SearchBar extends Component {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
    }

    this.handleSearch = ::this.handleSearch
    this.handleChange = ::this.handleChange
  }

  componentWillMount() {
    const waitPeriod = 300
    this.handleSearch = _.debounce(this.handleSearch, waitPeriod)
  }

  handleSearch() {
    this.props.onSearch(this.state.searchTerm)
  }

  handleChange(e) {
    this.setState({searchTerm: e.target.value}, this.handleSearch)
  }

  render() {
    return (
      <div className="users__search-widget input-group">
        <input
          type="text"
          className="form-control"
          placeholder="Filter Alerts by Name..."
          onChange={this.handleChange}
          value={this.state.searchTerm}
        />
        <div className="input-group-addon">
          <span className="icon search" />
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

AlertsTable.propTypes = {
  alerts: arrayOf(
    shape({
      name: string,
      time: string,
      value: string,
      host: string,
      level: string,
    })
  ),
  source: shape({
    id: string.isRequired,
    name: string.isRequired,
  }).isRequired,
  shouldNotBeFilterable: bool,
  limit: number,
  onGetMoreAlerts: func,
  isAlertsMaxedOut: bool,
  alertsCount: number,
}

SearchBar.propTypes = {
  onSearch: func.isRequired,
}

export default AlertsTable
