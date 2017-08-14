import React, {PropTypes} from 'react'
import {Link} from 'react-router'
import _ from 'lodash'

import shallowCompare from 'react-addons-shallow-compare'
import classnames from 'classnames'
import {HOSTS_TABLE} from 'src/hosts/constants/tableSizing'

const {arrayOf, bool, number, shape, string} = PropTypes

const HostsTable = React.createClass({
  propTypes: {
    hosts: arrayOf(
      shape({
        name: string,
        cpu: number,
        load: number,
        apps: arrayOf(string.isRequired),
      })
    ),
    hostsLoading: bool,
    hostsError: string,
    source: shape({
      id: string.isRequired,
      name: string.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      searchTerm: '',
      sortDirection: null,
      sortKey: null,
    }
  },

  filter(allHosts, searchTerm) {
    const filterText = searchTerm.toLowerCase()
    return allHosts.filter(h => {
      const apps = h.apps ? h.apps.join(', ') : ''
      // search each tag for the presence of the search term
      let tagResult = false
      if (h.tags) {
        tagResult = Object.keys(h.tags).reduce((acc, key) => {
          return acc || h.tags[key].toLowerCase().includes(filterText)
        }, false)
      } else {
        tagResult = false
      }
      return (
        h.name.toLowerCase().includes(filterText) ||
        apps.toLowerCase().includes(filterText) ||
        tagResult
      )
    })
  },

  sort(hosts, key, direction) {
    switch (direction) {
      case 'asc':
        return _.sortBy(hosts, e => e[key])
      case 'desc':
        return _.sortBy(hosts, e => e[key]).reverse()
      default:
        return hosts
    }
  },

  updateSearchTerm(term) {
    this.setState({searchTerm: term})
  },

  updateSort(key) {
    // if we're using the key, reverse order; otherwise, set it with ascending
    if (this.state.sortKey === key) {
      const reverseDirection =
        this.state.sortDirection === 'asc' ? 'desc' : 'asc'
      this.setState({sortDirection: reverseDirection})
    } else {
      this.setState({sortKey: key, sortDirection: 'asc'})
    }
  },

  sortableClasses(key) {
    if (this.state.sortKey === key) {
      if (this.state.sortDirection === 'asc') {
        return 'sortable-header sorting-ascending'
      }
      return 'sortable-header sorting-descending'
    }
    return 'sortable-header'
  },

  render() {
    const {searchTerm, sortKey, sortDirection} = this.state
    const {hosts, hostsLoading, hostsError, source} = this.props
    const sortedHosts = this.sort(
      this.filter(hosts, searchTerm),
      sortKey,
      sortDirection
    )
    const hostCount = sortedHosts.length
    const {colName, colStatus, colCPU, colLoad} = HOSTS_TABLE

    let hostsTitle

    if (hostsLoading) {
      hostsTitle = 'Loading Hosts...'
    } else if (hostsError.length) {
      hostsTitle = 'There was a problem loading hosts'
    } else if (hostCount === 1) {
      hostsTitle = `${hostCount} Host`
    } else {
      hostsTitle = `${hostCount} Hosts`
    }

    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">
            {hostsTitle}
          </h2>
          <SearchBar onSearch={this.updateSearchTerm} />
        </div>
        <div className="panel-body">
          {hostCount > 0 && !hostsError.length
            ? <table className="table v-center table-highlight">
                <thead>
                  <tr>
                    <th
                      onClick={() => this.updateSort('name')}
                      className={this.sortableClasses('name')}
                      style={{width: colName}}
                    >
                      Host
                    </th>
                    <th
                      onClick={() => this.updateSort('deltaUptime')}
                      className={this.sortableClasses('deltaUptime')}
                      style={{width: colStatus}}
                    >
                      Status
                    </th>
                    <th
                      onClick={() => this.updateSort('cpu')}
                      className={this.sortableClasses('cpu')}
                      style={{width: colCPU}}
                    >
                      CPU
                    </th>
                    <th
                      onClick={() => this.updateSort('load')}
                      className={this.sortableClasses('load')}
                      style={{width: colLoad}}
                    >
                      Load
                    </th>
                    <th>Apps</th>
                  </tr>
                </thead>

                <tbody>
                  {sortedHosts.map(h => {
                    return <HostRow key={h.name} host={h} source={source} />
                  })}
                </tbody>
              </table>
            : <div className="generic-empty-state">
                <h4 style={{margin: '90px 0'}}>No Hosts found</h4>
              </div>}
        </div>
      </div>
    )
  },
})

const HostRow = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
    }).isRequired,
    host: PropTypes.shape({
      name: PropTypes.string,
      cpu: PropTypes.number,
      load: PropTypes.number,
      deltaUptime: PropTypes.number.required,
      apps: PropTypes.arrayOf(PropTypes.string.isRequired),
    }),
  },

  shouldComponentUpdate(nextProps) {
    return shallowCompare(this, nextProps)
  },

  render() {
    const {host, source} = this.props
    const {name, cpu, load, apps = []} = host
    const {colName, colStatus, colCPU, colLoad} = HOSTS_TABLE

    return (
      <tr>
        <td style={{width: colName}}>
          <Link to={`/sources/${source.id}/hosts/${name}`}>
            {name}
          </Link>
        </td>
        <td style={{width: colStatus}}>
          <div
            className={classnames(
              'table-dot',
              Math.max(host.deltaUptime || 0, host.winDeltaUptime || 0) > 0
                ? 'dot-success'
                : 'dot-critical'
            )}
          />
        </td>
        <td style={{width: colCPU}} className="monotype">
          {isNaN(cpu) ? 'N/A' : `${cpu.toFixed(2)}%`}
        </td>
        <td style={{width: colLoad}} className="monotype">
          {isNaN(load) ? 'N/A' : `${load.toFixed(2)}`}
        </td>
        <td>
          {apps.map((app, index) => {
            return (
              <span key={app}>
                <Link
                  style={{marginLeft: '2px'}}
                  to={{
                    pathname: `/sources/${source.id}/hosts/${name}`,
                    query: {app},
                  }}
                >
                  {app}
                </Link>
                {index === apps.length - 1 ? null : ', '}
              </span>
            )
          })}
        </td>
      </tr>
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

  handleChange() {
    this.setState({searchTerm: this.refs.searchInput.value}, this.handleSearch)
  },

  render() {
    return (
      <div className="users__search-widget input-group">
        <input
          type="text"
          className="form-control"
          placeholder="Filter by Host..."
          ref="searchInput"
          onChange={this.handleChange}
        />
        <div className="input-group-addon">
          <span className="icon search" aria-hidden="true" />
        </div>
      </div>
    )
  },
})

export default HostsTable
