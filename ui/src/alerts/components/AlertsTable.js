import React, {Component, PropTypes} from 'react'

import _ from 'lodash'
import classnames from 'classnames'
import {Link} from 'react-router'
import uuid from 'node-uuid'

import InfiniteScroll from 'shared/components/InfiniteScroll'

import {ALERTS_TABLE} from 'src/alerts/constants/tableSizing'

class AlertsTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
      filteredAlerts: this.props.alerts,
      sortDirection: null,
      sortKey: null,
    }
  }

  componentWillReceiveProps(newProps) {
    this.filterAlerts(this.state.searchTerm, newProps.alerts)
  }

  filterAlerts = (searchTerm, newAlerts) => {
    const alerts = newAlerts || this.props.alerts
    const filterText = searchTerm.toLowerCase()
    const filteredAlerts = alerts.filter(({name, host, level}) => {
      return (
        (name && name.toLowerCase().includes(filterText)) ||
        (host && host.toLowerCase().includes(filterText)) ||
        (level && level.toLowerCase().includes(filterText))
      )
    })
    this.setState({searchTerm, filteredAlerts})
  }

  changeSort = key => () => {
    // if we're using the key, reverse order; otherwise, set it with ascending
    if (this.state.sortKey === key) {
      const reverseDirection =
        this.state.sortDirection === 'asc' ? 'desc' : 'asc'
      this.setState({sortDirection: reverseDirection})
    } else {
      this.setState({sortKey: key, sortDirection: 'asc'})
    }
  }

  sortableClasses = key => {
    if (this.state.sortKey === key) {
      if (this.state.sortDirection === 'asc') {
        return 'alert-history-table--th sortable-header sorting-ascending'
      }
      return 'alert-history-table--th sortable-header sorting-descending'
    }
    return 'alert-history-table--th sortable-header'
  }

  sort = (alerts, key, direction) => {
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
    const {colName, colLevel, colTime, colHost, colValue} = ALERTS_TABLE
    return this.props.alerts.length
      ? <div className="alert-history-table">
          <div className="alert-history-table--thead">
            <div
              onClick={this.changeSort('name')}
              className={this.sortableClasses('name')}
              style={{width: colName}}
            >
              Name
            </div>
            <div
              onClick={this.changeSort('level')}
              className={this.sortableClasses('level')}
              style={{width: colLevel}}
            >
              Level
            </div>
            <div
              onClick={this.changeSort('time')}
              className={this.sortableClasses('time')}
              style={{width: colTime}}
            >
              Time
            </div>
            <div
              onClick={this.changeSort('host')}
              className={this.sortableClasses('host')}
              style={{width: colHost}}
            >
              Host
            </div>
            <div
              onClick={this.changeSort('value')}
              className={this.sortableClasses('value')}
              style={{width: colValue}}
            >
              Value
            </div>
          </div>
          <InfiniteScroll
            className="alert-history-table--tbody"
            itemHeight={25}
            items={alerts.map(({name, level, time, host, value}) => {
              return (
                <div className="alert-history-table--tr" key={uuid.v4()}>
                  <div
                    className="alert-history-table--td"
                    style={{width: colName}}
                  >
                    {name}
                  </div>
                  <div
                    className={`alert-history-table--td alert-level-${level.toLowerCase()}`}
                    style={{width: colLevel}}
                  >
                    <span
                      className={classnames(
                        'table-dot',
                        {'dot-critical': level === 'CRITICAL'},
                        {'dot-success': level === 'OK'}
                      )}
                    />
                  </div>
                  <div
                    className="alert-history-table--td"
                    style={{width: colTime}}
                  >
                    {new Date(Number(time)).toISOString()}
                  </div>
                  <div
                    className="alert-history-table--td alert-history-table--host"
                    style={{width: colHost}}
                  >
                    <Link to={`/sources/${id}/hosts/${host}`} title={host}>
                      {host}
                    </Link>
                  </div>
                  <div
                    className="alert-history-table--td"
                    style={{width: colValue}}
                  >
                    {value}
                  </div>
                </div>
              )
            })}
          />
        </div>
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
          <h4 className="no-user-select">There are no Alerts to display</h4>
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
            <h2 className="panel-title">
              {this.props.alerts.length} Alerts
            </h2>
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
          placeholder="Filter Alerts..."
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
