import React, {PropTypes} from 'react';
import _ from 'lodash';

const AlertsTable = React.createClass({
  propTypes: {
    alerts: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string,
      time: PropTypes.string,
      value: PropTypes.string,
      host: PropTypes.string,
      severity: PropTypes.string,
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
    };
  },

  componentWillReceiveProps(newProps) {
    this.filterAlerts(newProps.alerts, this.state.searchTerm);
  },

  filterAlerts(allAlerts, searchTerm) {
    const alerts = allAlerts.filter((h) => h.name.search(searchTerm) !== -1);
    this.setState({searchTerm, filteredAlerts: alerts});
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

  sort(alerts, key, direction) {
    switch (direction) {
      case 'asc':
        return _.sortBy(alerts, (e) => e[key]);
      case 'desc':
        return _.sortBy(alerts, (e) => e[key]).reverse();
      default:
        return alerts;
    }
  },

  render() {
    const alerts = this.sort(this.state.filteredAlerts, this.state.sortKey, this.state.sortDirection);
    const {source} = this.props;

    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-ai-center u-jc-space-between">
          <h2 className="panel-title">{this.props.alerts.length} Alerts</h2>
          <SearchBar onSearch={_.wrap(this.props.alerts, this.filterAlerts)} />
        </div>
        <div className="panel-body">
          <table className="table v-center">
            <thead>
              <tr>
                <th onClick={() => this.changeSort('name')} className="sortable-header">Alert</th>
                <th className="text-center">Time</th>
                <th onClick={() => this.changeSort('value')} className="sortable-header">Value</th>
                <th onClick={() => this.changeSort('host')} className="sortable-header">Host</th>
                <th onClick={() => this.changeSort('severity')} className="sortable-header">Severity</th>
                <th>Apps</th>
              </tr>
            </thead>
            <tbody>
              {
                alerts.map(({name, cpu, load}) => {
                  return (
                    <tr key={name}>
                      <td className="monotype"><a href={`/sources/${source.id}/alerts/${name}`}>{name}</a></td>
                      <td className="text-center"><div className="table-dot dot-success"></div></td>
                      <td className="monotype">{`${cpu.toFixed(2)}%`}</td>
                      <td className="monotype">{`${load.toFixed(2)}`}</td>
                      <td className="monotype">influxdb, ntp, system</td>
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
          placeholder="Filter Alerts"
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

export default AlertsTable;
