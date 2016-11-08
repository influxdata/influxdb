import React, {PropTypes} from 'react';
import ClusterAccountsPage from '../components/ClusterAccountsPage';
import DeleteClusterAccountModal from '../components/DeleteClusterAccountModal';
import {buildClusterAccounts} from 'src/shared/presenters';
import {
  getClusterAccounts,
  getRoles,
  deleteClusterAccount,
  getWebUsersByClusterAccount,
  meShow,
  addUsersToRole,
  createClusterAccount,
} from 'src/shared/apis';
import _ from 'lodash';

export const ClusterAccountsPageContainer = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      users: [],
      roles: [],

      // List of associated web users to display when deleting a cluster account.
      webUsers: [],

      // This is an unfortunate solution to using bootstrap to open modals.
      // The modal will have already been rendered in this component by the
      // time a user chooses "Remove" from one of the rows in the users table.
      userToDelete: null,
    };
  },

  componentDidMount() {
    const {clusterID} = this.props.params;
    Promise.all([
      getClusterAccounts(clusterID),
      getRoles(clusterID),
      meShow(),
    ]).then(([accountsResp, rolesResp, me]) => {
      this.setState({
        users: buildClusterAccounts(accountsResp.data.users, rolesResp.data.roles),
        roles: rolesResp.data.roles,
        me: me.data,
      });
    });
  },

  // Ensures the modal will remove the correct user. TODO: our own modals
  handleDeleteAccount(account) {
    getWebUsersByClusterAccount(this.props.params.clusterID, account.name).then(resp => {
      this.setState({
        webUsers: resp.data,
        userToDelete: account,
      });
    }).catch(err => {
      console.error(err.toString()); // eslint-disable-line no-console
      this.props.addFlashMessage({
        type: 'error',
        text: 'An error occured while trying to remove a cluster account.',
      });
    });
  },

  handleDeleteConfirm() {
    const {name} = this.state.userToDelete;
    deleteClusterAccount(this.props.params.clusterID, name).then(() => {
      this.props.addFlashMessage({
        type: 'success',
        text: 'Cluster account deleted!',
      });

      this.setState({
        users: _.reject(this.state.users, (user) => user.name === name),
      });
    }).catch((err) => {
      console.error(err.toString()); // eslint-disable-line no-console
      this.props.addFlashMessage({
        type: 'error',
        text: 'An error occured while trying to remove a cluster account.',
      });
    });
  },

  handleCreateAccount(name, password, roleName) {
    const {clusterID} = this.props.params;
    const {users, roles} = this.state;
    createClusterAccount(clusterID, name, password).then(() => {
      addUsersToRole(clusterID, roleName, [name]).then(() => {
        this.props.addFlashMessage({
          type: 'success',
          text: `User ${name} added with the ${roleName} role`,
        });

        // add user to role
        const newRoles = roles.map((role) => {
          if (role.name !== roleName) {
            return role;
          }

          return Object.assign({}, role, {
            users: role.users ? role.users.concat(name) : [name],
          });
        });

        const newUser = buildClusterAccounts([{name}], newRoles);
        this.setState({
          roles: newRoles,
          users: users.concat(newUser),
        });
      }).catch((err) => {
        console.error(err.toString()); // eslint-disable-line no-console
        this.props.addFlashMessage({
          type: 'error',
          text: `An error occured while assigning ${name} to the ${roleName} role`,
        });
      });
    }).catch((err) => {
      const msg = _.get(err, 'response.data.error', '');
      console.error(err.toString()); // eslint-disable-line no-console
      this.props.addFlashMessage({
        type: 'error',
        text: `An error occured creating user ${name}. ${msg}`,
      });
    });
  },

  render() {
    const {clusterID} = this.props.params;
    const {users, me, roles} = this.state;

    return (
      <div>
        <ClusterAccountsPage
          users={users}
          roles={roles}
          clusterID={clusterID}
          onDeleteAccount={this.handleDeleteAccount}
          onCreateAccount={this.handleCreateAccount}
          me={me}
        />
        <DeleteClusterAccountModal
          account={this.state.userToDelete}
          webUsers={this.state.webUsers}
          onConfirm={this.handleDeleteConfirm}
        />
      </div>
    );
  },
});

export default ClusterAccountsPageContainer;
