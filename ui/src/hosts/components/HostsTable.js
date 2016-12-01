import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import _ from 'lodash';

const HostsTable = React.createClass({
  propTypes: {
    hosts: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string,
      cpu: PropTypes.number,
      load: PropTypes.number,
      apps: PropTypes.arrayOf(PropTypes.string.isRequired),
    })),
    source: PropTypes.shape({
      id: PropTypes.string.isRequired,
      name: PropTypes.string.isRequired,
    }).isRequired,
  },

  getInitialState() {
    return {
      searchTerm: '',
      filteredHosts: this.props.hosts,
      sortDirection: null,
      sortKey: null,
    };
  },

  componentWillReceiveProps(newProps) {
    this.filterHosts(newProps.hosts, this.state.searchTerm);
  },

  filterHosts(allHosts, searchTerm) {
    const hosts = allHosts.filter((h) => {
      let apps = null;
      if (h.apps) {
        apps = h.apps.join(', ');
      } else {
        apps = '';
      }
      // search each tag for the presence of the search term
      let tagResult = false;
      if (h.tags) {
        tagResult = Object.keys(h.tags).reduce((acc, key) => {
          return acc || h.tags[key].search(searchTerm) !== -1;
        }, false);
      } else {
        tagResult = false;
      }
      return (
        h.name.search(searchTerm) !== -1 ||
        apps.search(searchTerm) !== -1 ||
        tagResult
      );
    });
    this.setState({searchTerm, filteredHosts: hosts});
  },

  changeSort(key) {
    // if we're using the key, reverse order; otherwise, set it with ascending
    if (this.state.sortKey === key) {
      const reverseDirection = (this.state.sortDirection === 'asc' ? 'desc' : 'asc');
      this.setState({sortDirection: reverseDirection});
    } else {
      this.setState({sortKey: key, sortDirection: 'asc'});
    }
  },

  sort(hosts, key, direction) {
    switch (direction) {
      case 'asc':
        return _.sortBy(hosts, (e) => e[key]);
      case 'desc':
        return _.sortBy(hosts, (e) => e[key]).reverse();
      default:
        return hosts;
    }
  },

  sortableClasses(key) {
    if (this.state.sortKey === key) {
      if (this.state.sortDirection === 'asc') {
        return "sortable-header sorting-up";
      }
      return "sortable-header sorting-down";
    }
    return "sortable-header";
  },

  render() {
    const hosts = this.sort(this.state.filteredHosts, this.state.sortKey, this.state.sortDirection);
    const hostCount = hosts.length;
    const {source} = this.props;

    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">{hostCount ? `${hostCount} Hosts` : ''}</h2>
          <SearchBar onSearch={_.wrap(this.props.hosts, this.filterHosts)} />
        </div>
        <div className="panel-body">
          <table className="table v-center">
            <thead>
              <tr>
                <th onClick={() => this.changeSort('name')} className={this.sortableClasses('name')}>Hostname</th>
                <th className="text-center">Status</th>
                <th onClick={() => this.changeSort('cpu')} className={this.sortableClasses('cpu')}>CPU</th>
                <th onClick={() => this.changeSort('load')} className={this.sortableClasses('load')}>Load</th>
                <th>Apps</th>
              </tr>
            </thead>
            <tbody>
              {
                hosts.map(({name, cpu, load, apps = []}) => {
                  return (
                    <tr key={name}>
                      <td className="monotype"><Link to={`/sources/${source.id}/hosts/${name}`}>{name}</Link></td>
                      <td className="text-center"><div className="table-dot dot-success"></div></td>
                      <td className="monotype">{isNaN(cpu) ? 'N/A' : `${cpu.toFixed(2)}%`}</td>
                      <td className="monotype">{isNaN(load) ? 'N/A' : `${load.toFixed(2)}`}</td>
                      <td className="monotype">
                        {apps.map((app, index) => {
                          return (
                            <span key={app}>
                              <Link
                                style={{marginLeft: "2px"}}
                                to={{pathname: `/sources/${source.id}/hosts/${name}`, query: {app}}}>
                                {app}
                              </Link>
                              {index === apps.length - 1 ? null : ', '}
                            </span>
                          );
                        })}
                      </td>
                    </tr>
                  );
                })
              }
            </tbody>
          </table>
        </div>
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
        <input
          type="text"
          className="form-control"
          placeholder="Filter Hosts"
          ref="searchInput"
          onChange={this.handleChange}
        />
        <div className="input-group-addon">
          <span className="icon search" aria-hidden="true"></span>
        </div>
      </div>
    );
  },
});

export default HostsTable;
