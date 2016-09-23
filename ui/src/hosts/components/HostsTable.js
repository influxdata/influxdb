import React, {PropTypes} from 'react';
import Reactable from 'reactable';

const HostsTable = React.createClass({
  propTypes: {
    hosts: PropTypes.arrayOf(React.PropTypes.object),
  },

  getInitialState() {
    return {
      filteredHosts: this.props.hosts,
    };
  },

  filterHosts(searchTerm) {
    const hosts = this.props.hosts.filter((h) => h.name.search(searchTerm) !== -1);
    this.setState({filteredHosts: hosts});
  },

  render() {
    const {Table, Thead, Tr, Td, Th} = Reactable;

    return (
      <div>
        <SearchBar onSearch={this.filterHosts} />
        <Table sortable={true} className="table v-center">
          <Thead>
            <Th column="name">Hostname</Th>
            <Th column="status">Status</Th>
            <Th column="cpu">CPU</Th>
            <Th column="load">Load</Th>
            <Th column="apps">Apps</Th>
          </Thead>
          {
            this.state.filteredHosts.map(({name, id}) => {
              return (
                <Tr key={id}>
                  <Td column="name"><a href={`/hosts/${id}`}>{name}</a></Td>
                  <Td column="status">UP</Td>
                  <Td column="cpu">98%</Td>
                  <Td column="load">1.12</Td>
                  <Td column="apps">influxdb, ntp, system</Td>
                </Tr>
              );
            })
          }
        </Table>
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
