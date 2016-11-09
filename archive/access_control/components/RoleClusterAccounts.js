import React, {PropTypes} from 'react';
import {Link} from 'react-router';

const RoleClusterAccounts = React.createClass({
  propTypes: {
    clusterID: PropTypes.string.isRequired,
    users: PropTypes.arrayOf(PropTypes.string.isRequired),
    onRemoveClusterAccount: PropTypes.func.isRequired,
  },

  getDefaultProps() {
    return {users: []};
  },

  getInitialState() {
    return {
      searchText: '',
    };
  },

  handleSearch(searchText) {
    this.setState({searchText});
  },

  handleRemoveClusterAccount(user) {
    this.props.onRemoveClusterAccount(user);
  },

  render() {
    const users = this.props.users.filter((user) => {
      const name = user.toLowerCase();
      const searchText = this.state.searchText.toLowerCase();
      return name.indexOf(searchText) > -1;
    });

    return (
      <div className="panel panel-default">
        <div className="panel-body cluster-accounts">
          {this.props.users.length ? <SearchBar onSearch={this.handleSearch} searchText={this.state.searchText} /> : null}
          {this.props.users.length ? (
            <TableBody
              users={users}
              clusterID={this.props.clusterID}
              onRemoveClusterAccount={this.handleRemoveClusterAccount}
            />
            ) : (
            <div className="generic-empty-state">
              <span className="icon alert-triangle"></span>
              <h4>No Cluster Accounts found</h4>
            </div>
            )}
        </div>
      </div>
    );
  },
});

const TableBody = React.createClass({
  propTypes: {
    users: PropTypes.arrayOf(PropTypes.string.isRequired),
    clusterID: PropTypes.string.isRequired,
    onRemoveClusterAccount: PropTypes.func.isRequired,
  },

  render() {
    return (
      <table className="table v-center users-table">
        <tbody>
          <tr>
            <th></th>
            <th>Username</th>
            <th></th>
          </tr>
          {this.props.users.map((user) => {
            return (
              <tr key={user} data-row-id={user}>
                <td></td>
                <td>
                  <Link to={`/clusters/${this.props.clusterID}/accounts/${user}`} className="btn btn-xs btn-link">
                    {user}
                  </Link>
                </td>
                <td>
                  <button
                    title="Remove cluster account from role"
                    onClick={() => this.props.onRemoveClusterAccount(user)}
                    type="button"
                    data-toggle="modal"
                    data-target="#removeAccountFromRoleModal"
                    className="btn btn-sm btn-link-danger"
                  >
                    Remove From Role
                  </button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  },
});

const SearchBar = React.createClass({
  propTypes: {
    onSearch: PropTypes.func.isRequired,
    searchText: PropTypes.string.isRequired,
  },

  handleChange() {
    this.props.onSearch(this._searchText.value);
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
          placeholder="Find User"
          value={this.props.searchText}
          ref={(ref) => this._searchText = ref}
          onChange={this.handleChange}
        />
      </div>
    );
  },
});

export default RoleClusterAccounts;
