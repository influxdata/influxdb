import React, {PropTypes, Component} from 'react'
import _ from 'lodash'

import SearchBar from 'src/hosts/components/SearchBar'
import HostRow from 'src/hosts/components/HostRow'
import InfiniteScroll from 'shared/components/InfiniteScroll'

import {HOSTS_TABLE} from 'src/hosts/constants/tableSizing'

class HostsTable extends Component {
  constructor(props) {
    super(props)

    this.state = {
      searchTerm: '',
      sortDirection: null,
      sortKey: null,
    }
  }

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
  }

  sort(hosts, key, direction) {
    switch (direction) {
      case 'asc':
        return _.sortBy(hosts, e => e[key])
      case 'desc':
        return _.sortBy(hosts, e => e[key]).reverse()
      default:
        return hosts
    }
  }

  updateSearchTerm = term => {
    this.setState({searchTerm: term})
  }

  updateSort = key => () => {
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
        return 'hosts-table--th sortable-header sorting-ascending'
      }
      return 'hosts-table--th sortable-header sorting-descending'
    }
    return 'hosts-table--th sortable-header'
  }

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
          <SearchBar
            placeholder="Filter by Host..."
            onSearch={this.updateSearchTerm}
          />
        </div>
        <div className="panel-body">
          {hostCount > 0 && !hostsError.length
            ? <div className="hosts-table">
                <div className="hosts-table--thead">
                  <div className="hosts-table--tr">
                    <div
                      onClick={this.updateSort('name')}
                      className={this.sortableClasses('name')}
                      style={{width: colName}}
                    >
                      Host
                    </div>
                    <div
                      onClick={this.updateSort('deltaUptime')}
                      className={this.sortableClasses('deltaUptime')}
                      style={{width: colStatus}}
                    >
                      Status
                    </div>
                    <div
                      onClick={this.updateSort('cpu')}
                      className={this.sortableClasses('cpu')}
                      style={{width: colCPU}}
                    >
                      CPU
                    </div>
                    <div
                      onClick={this.updateSort('load')}
                      className={this.sortableClasses('load')}
                      style={{width: colLoad}}
                    >
                      Load
                    </div>
                    <div className="hosts-table--th">Apps</div>
                  </div>
                </div>
                <InfiniteScroll
                  items={sortedHosts.map(h =>
                    <HostRow key={h.name} host={h} source={source} />
                  )}
                  itemHeight={26}
                  className="hosts-table--tbody"
                />
              </div>
            : <div className="generic-empty-state">
                <h4 style={{margin: '90px 0'}}>No Hosts found</h4>
              </div>}
        </div>
      </div>
    )
  }
}

const {arrayOf, bool, number, shape, string} = PropTypes

HostsTable.propTypes = {
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
}

export default HostsTable
