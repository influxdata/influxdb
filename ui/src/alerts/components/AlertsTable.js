import React, {PropTypes} from 'react'
import _ from 'lodash'
import {Link} from 'react-router'

const AlertsTable = React.createClass({
  propTypes: {
    alerts: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string,
      time: PropTypes.string,
      value: PropTypes.string,
      host: PropTypes.string,
      level: PropTypes.string,
    })),
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      searchTerm: '',
      filteredAlerts: this.props.alerts,
      sortDirection: null,
      sortKey: null,
    }
  },

  componentWillReceiveProps(newProps) {
    this.filterAlerts(this.state.searchTerm, newProps.alerts)
  },

  filterAlerts(searchTerm, newAlerts) {
    const alerts = newAlerts || this.props.alerts
    const filteredAlerts = alerts.filter((h) => {
      if (h.host === null || h.name === null || h.level === null) {
        return false
      }

      return h.name.toLowerCase().search((searchTerm).toLowerCase()) !== -1 ||
        h.host.toLowerCase().search((searchTerm).toLowerCase()) !== -1 ||
        h.level.toLowerCase().search((searchTerm).toLowerCase()) !== -1
    })
    this.setState({searchTerm, filteredAlerts})
  },

  changeSort(key) {
    // if we're using the key, reverse order; otherwise, set it with ascending
    if (this.state.sortKey === key) {
      const reverseDirection = (this.state.sortDirection === 'asc' ? 'desc' : 'asc')
      this.setState({sortDirection: reverseDirection})
    } else {
      this.setState({sortKey: key, sortDirection: 'asc'})
    }
  },

  sort(alerts, key, direction) {
    switch (direction) {
      case 'asc':
        return _.sortBy(alerts, (e) => e[key])
      case 'desc':
        return _.sortBy(alerts, (e) => e[key]).reverse()
      default:
        return alerts
    }
  },

  render() {
    const {id} = this.props.source
    const alerts = this.sort(this.state.filteredAlerts, this.state.sortKey, this.state.sortDirection)
    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">{this.props.alerts.length} Alerts</h2>
          <SearchBar onSearch={this.filterAlerts}/>
        </div>
        <div className="panel-body">
          <table className="table v-center">
            <thead>
              <tr>
                <th onClick={() => this.changeSort('name')} className="sortable-header">Name</th>
                <th onClick={() => this.changeSort('level')} className="sortable-header">Level</th>
                <th onClick={() => this.changeSort('time')} className="sortable-header">Time</th>
                <th onClick={() => this.changeSort('host')} className="sortable-header">Host</th>
                <th onClick={() => this.changeSort('value')} className="sortable-header">Value</th>
              </tr>
            </thead>
            <tbody>
              {
                alerts.map(({name, level, time, host, value}) => {
                  return (
                    <tr key={`${name}-${level}-${time}-${host}-${value}`}>
                      <td className="monotype">{name}</td>
                      <td className={`monotype alert-level-${level.toLowerCase()}`}>{level}</td>
                      <td className="monotype">{(new Date(Number(time)).toISOString())}</td>
                      <td className="monotype">
                        <Link to={`/sources/${id}/hosts/${host}`}>
                          {host}
                        </Link>
                      </td>
                      <td className="monotype">{value}</td>
                    </tr>
                  )
                })
              }
            </tbody>
          </table>
        </div>
      </div>
    )
  },
})

const SearchBar = React.createClass({
  propTypes: {
    onSearch: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      searchTerm: '',
    }
  },

  componentWillMount() {
    const waitPeriod = 300
    this.handleSearch = _.debounce(this.handleSearch, waitPeriod)
  },

  handleSearch() {
    this.props.onSearch(this.state.searchTerm)
  },

  handleChange(e) {
    this.setState({searchTerm: e.target.value}, this.handleSearch)
  },

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
          <span className="icon search" aria-hidden="true"></span>
        </div>
      </div>
    )
  },
})

export default AlertsTable
