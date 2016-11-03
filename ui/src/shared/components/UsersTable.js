import React, {PropTypes} from 'react';
import {Link} from 'react-router';
import classNames from 'classnames';

const {func, shape, arrayOf, string} = PropTypes;
const UsersTable = React.createClass({
  propTypes: {
    users: arrayOf(shape({}).isRequired).isRequired,
    activeCluster: string.isRequired,
    onUserToDelete: func.isRequired,
    me: shape({}).isRequired,
    deleteText: string,
  },

  getDefaultProps() {
    return {
      deleteText: 'Delete',
    };
  },

  handleSelectUserToDelete(user) {
    this.props.onUserToDelete(user);
  },
  render() {
    const {users, activeCluster, me} = this.props;

    if (!users.length) {
      return (
        <div className="generic-empty-state">
          <span className="icon user-outline"/>
          <h4>No users</h4>
        </div>
      );
    }

    return (
      <table className="table v-center users-table">
        <tbody>
          <tr>
            <th></th>
            <th>Name</th>
            <th>Admin</th>
            <th>Email</th>
            <th></th>
          </tr>
          {
            users.map((user) => {
              const isMe = me.id === user.id;
              return (
                <tr key={user.id}>
                  <td></td>
                  <td>
                    <span>
                      <Link to={`/clusters/${activeCluster}/users/${user.id}`} title={`Go to ${user.name}'s profile`}>{user.name}</Link>
                      {isMe ? <em> (You) </em> : null}
                    </span>
                  </td>
                  <td className="admin-column">{this.renderAdminIcon(user.admin)}</td>
                  <td>{user.email}</td>
                  <td>
                    {this.renderDeleteButton(user)}
                  </td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  },

  renderAdminIcon(isAdmin) {
    return <span className={classNames("icon", {"checkmark text-color-success": isAdmin, "remove text-color-danger": !isAdmin})}></span>;
  },

  renderDeleteButton(user) {
    if (this.props.me.id === user.id) {
      return <button type="button" className="btn btn-sm btn-link-danger disabled" title={`Cannot ${this.props.deleteText} Yourself`}>{this.props.deleteText}</button>;
    }

    return (
      <button
        onClick={() => this.handleSelectUserToDelete({id: user.id, name: user.name})}
        type="button"
        data-toggle="modal"
        data-target="#deleteUsersModal"
        className="btn btn-sm btn-link-danger"
      >
        {this.props.deleteText}
      </button>
    );
  },
});

export default UsersTable;
