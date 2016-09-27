import React, {PropTypes} from 'react';
import _ from 'lodash';

const HostsTable = React.createClass({
  propTypes: {
    hosts: PropTypes.arrayOf(React.PropTypes.object),
  },

  getInitialState() {
    return {
      filteredHosts: this.props.hosts,
      sortDirection: null,
    };
  },

  filterHosts(searchTerm) {
    const unfiltered = this.props.hosts;
    const hosts = unfiltered.filter((h) => h.name.search(searchTerm) !== -1);
    this.setState({filteredHosts: hosts});
  },

  changeSort() {
    if (this.state.sortDirection === 'asc') {
      this.setState({sortDirection: 'desc'});
    } else {
      this.setState({sortDirection: 'asc'});
    }
  },

  sort(hosts) {
    switch (this.state.sortDirection) {
      case 'asc':
        return _.sortBy(hosts, (e) => e.name);
      case 'desc':
        return _.sortBy(hosts, (e) => e.name).reverse();
      default:
        return hosts;
    }
  },

  render() {
    const hosts = this.sort(this.state.filteredHosts);

    return (
      <div>
        <SearchBar onSearch={this.filterHosts} />
        <table className="table v-center">
          <thead>
            <tr>
              <th onClick={this.changeSort} className="sortable-header">Hostname</th>
              <th>Status</th>
              <th>CPU</th>
              <th>Load</th>
              <th>Apps</th>
            </tr>
          </thead>
          <tbody>
            {
              hosts.map(({name, id}) => {
                return (
                  <tr key={name}>
                    <td><a href={`/hosts/${id}`}>{name}</a></td>
                    <td>UP</td>
                    <td>98%</td>
                    <td>1.12</td>
                    <td>influxdb, ntp, system</td>
                  </tr>
                );
              })
            }
          </tbody>
        </table>
      </div>
    );
  },
});

const SearchBar = React.createClass({
  propTypes: {
    onSearch: PropTypes.func.isRequired,
  },

  handleChange() {
    this.props.onSearch(this.refs.searchInput.value);
  },

  render() {
    return (
      <div className="users__search-widget input-group">
        <div className="input-group-addon">
          <span className="icon search" aria-hidden="true"></span>
        </div>
        <input
          type="text"
          className="form-control"
          placeholder="Find host"
          ref="searchInput"
          onChange={this.handleChange}
        />
      </div>
    );
  },
});

export default HostsTable;
