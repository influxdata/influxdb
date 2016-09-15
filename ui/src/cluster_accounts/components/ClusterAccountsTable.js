import React, {PropTypes} from 'react';
import {Link} from 'react-router';

const ClusterAccountsTable = React.createClass({
  propTypes: {
    clusterID: PropTypes.string.isRequired,
    users: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string.isRequired,
      roles: PropTypes.arrayOf(PropTypes.shape({
        name: PropTypes.string.isRequired,
      })).isRequired,
    })),
    onDeleteAccount: PropTypes.func.isRequired,
    me: PropTypes.shape(),
  },

  getInitialState() {
    return {
      searchText: '',
    };
  },

  handleSearch(searchText) {
    this.setState({searchText});
  },

  handleDeleteAccount(user) {
    this.props.onDeleteAccount(user);
  },

  render() {
    const users = this.props.users.filter((user) => {
      const name = user.name.toLowerCase();
      const searchText = this.state.searchText.toLowerCase();
      return name.indexOf(searchText) > -1;
    });

    return (
      <div className="panel panel-minimal">
        <div className="panel-heading u-flex u-jc-space-between u-ai-center">
          <h2 className="panel-title">Cluster Accounts</h2>
          <SearchBar onSearch={this.handleSearch} searchText={this.state.searchText} />
        </div>

        <div className="panel-body">
          <TableBody
            users={users}
            clusterID={this.props.clusterID}
            onDeleteAccount={this.handleDeleteAccount}
            me={this.props.me}
          />
        </div>
      </div>
    );
  },
});

const TableBody = React.createClass({
  propTypes: {
    users: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string.isRequired,
      roles: PropTypes.arrayOf(PropTypes.shape({
        name: PropTypes.string.isRequired,
      })).isRequired,
    })),
    clusterID: PropTypes.string.isRequired,
    onDeleteAccount: PropTypes.func.isRequired,
    me: PropTypes.shape(),
  },

  render() {
    if (!this.props.users.length) {
      return (
        <div className="generic-empty-state">
          <span className="icon alert-triangle"></span>
          <h4>No Cluster Accounts</h4>
        </div>
      );
    }

    return (
      <table className="table v-center users-table">
        <tbody>
          <tr>
            <th>Username</th>
            <th>Roles</th>
            <th></th>
          </tr>
          {this.props.users.map((user) => {
            return (
              <tr key={user.name} data-test="user-row">
                <td>
                  <Link to={`/clusters/${this.props.clusterID}/accounts/${user.name}`} >
                    {user.name}
                  </Link>
                </td>
                <td>{user.roles.map((r) => r.name).join(', ')}</td>
                <td>
                  {this.renderDeleteAccount(user)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  },

  renderDeleteAccount(clusterAccount) {
    const currentUserIsAssociatedWithAccount = this.props.me.cluster_links.some(cl => (
      cl.cluster_user === clusterAccount.name
    ));
    const title = currentUserIsAssociatedWithAccount ?
      'You can\'t remove a cluster account that you are associated with.'
      : 'Delete cluster account';

    return (
      <button
        onClick={() => this.props.onDeleteAccount(clusterAccount)}
        title={title}
        type="button"
        data-toggle="modal"
        data-target="#deleteClusterAccountModal"
        className="btn btn-sm btn-link"
        disabled={currentUserIsAssociatedWithAccount}>
        Delete
      </button>
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

export default ClusterAccountsTable;
