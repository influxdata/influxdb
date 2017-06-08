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

  render() {
    const {id} = this.props.source
    const alerts = this.sort(
      this.state.filteredAlerts,
      this.state.sortKey,
      this.state.sortDirection
    )
    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">{this.props.alerts.length} Alerts</h2>
          {this.props.alerts.length
            ? <SearchBar onSearch={this.filterAlerts} />
            : null}
        </div>
        <div className="panel-body">
          {this.props.alerts.length
            ? <table className="table v-center table-highlight">
                <thead>
                  <tr>
                    <th
                      onClick={() => this.changeSort('name')}
                      className="sortable-header"
                    >
                      Name
                    </th>
                    <th
                      onClick={() => this.changeSort('level')}
                      className="sortable-header"
                    >
                      Level
                    </th>
                    <th
                      onClick={() => this.changeSort('time')}
                      className="sortable-header"
                    >
                      Time
                    </th>
                    <th
                      onClick={() => this.changeSort('host')}
                      className="sortable-header"
                    >
                      Host
                    </th>
                    <th
                      onClick={() => this.changeSort('value')}
                      className="sortable-header"
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
                        <td
                          className={`monotype alert-level-${level.toLowerCase()}`}
                        >
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
            : <div className="generic-empty-state">
                <h5 className="no-user-select">
                  Alerts appear here when you have Rules
                </h5>
                <br />
                <Link
                  to={`/sources/${id}/alert-rules/new`}
                  className="btn btn-primary"
                >
                  Create a Rule
                </Link>
              </div>}
        </div>
      </div>
    )
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

const {arrayOf, func, shape, string} = PropTypes

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
}

SearchBar.propTypes = {
  onSearch: func.isRequired,
}

export default AlertsTable
