import React, {PropTypes} from 'react';
import _ from 'lodash';
import {withRouter} from 'react-router';
import ClusterAccountEditPage from '../components/ClusterAccountEditPage';
import {buildClusterAccounts, buildRoles, buildAllPermissions, buildPermission} from 'src/shared/presenters';
import {showDatabases} from 'src/shared/apis/metaQuery';
import showDatabasesParser from 'shared/parsing/showDatabases';
import {
  addPermissionToAccount,
  removePermissionFromAccount,
  deleteUserClusterLink,
  getUserClusterLinks,
  getClusterAccount,
  getWebUsers,
  getRoles,
  addWebUsersToClusterAccount,
  updateClusterAccountPassword,
  removeAccountsFromRole,
  addAccountsToRole,
  meShow,
  deleteClusterAccount,
  getWebUsersByClusterAccount,
} from 'shared/apis';

const {shape, string, func, arrayOf} = PropTypes;

export const ClusterAccountContainer = React.createClass({
  propTypes: {
    dataNodes: arrayOf(string.isRequired),
    params: shape({
      clusterID: string.isRequired,
      accountID: string.isRequired,
    }).isRequired,
    router: shape({
      push: func.isRequired,
    }).isRequired,
    addFlashMessage: func,
  },

  getInitialState() {
    return {
      account: null,
      roles: [],
      databases: [],
      assignedWebUsers: [],
      unassignedWebUsers: [],
      me: {},
    };
  },

  componentDidMount() {
    const {accountID, clusterID} = this.props.params;
    const {dataNodes} = this.props;

    Promise.all([
      getClusterAccount(clusterID, accountID),
      getRoles(clusterID),
      showDatabases(dataNodes, clusterID),
      getWebUsersByClusterAccount(clusterID, accountID),
      getWebUsers(clusterID),
      meShow(),
    ]).then(([
      {data: {users}},
      {data: {roles}},
      {data: dbs},
      {data: assignedWebUsers},
      {data: allUsers},
      {data: me},
    ]) => {
      const account = buildClusterAccounts(users, roles)[0];
      const presentedRoles = buildRoles(roles);
      this.setState({
        account,
        assignedWebUsers,
        roles: presentedRoles,
        databases: showDatabasesParser(dbs).databases,
        unassignedWebUsers: _.differenceBy(allUsers, assignedWebUsers, (u) => u.id),
        me,
      });
    }).catch(err => {
      this.props.addFlashMessage({
        type: 'error',
        text: `An error occured. Please try refreshing the page.  ${err.message}`,
      });
    });
  },

  handleUpdatePassword(password) {
    updateClusterAccountPassword(this.props.params.clusterID, this.state.account.name, password).then(() => {
      this.props.addFlashMessage({
        type: 'success',
        text: 'Password successfully updated :)',
      });
    }).catch(() => {
      this.props.addFlashMessage({
        type: 'error',
        text: 'There was a problem updating password :(',
      });
    });
  },

  handleAddPermission({name, resources}) {
    const {clusterID} = this.props.params;
    const {account} = this.state;
    addPermissionToAccount(clusterID, account.name, name, resources).then(() => {
      const newPermissions = account.permissions.map(p => p.name).includes(name) ?
        account.permissions
        : account.permissions.concat(buildPermission(name, resources));

      this.setState({
        account: Object.assign({}, account, {permissions: newPermissions}),
      }, () => {
        this.props.addFlashMessage({
          type: 'success',
          text: 'Permission successfully added :)',
        });
      });
    }).catch(() => {
      this.props.addFlashMessage({
        type: 'error',
        text: 'There was a problem adding the permission :(',
      });
    });
  },

  handleRemovePermission(permission) {
    const {clusterID} = this.props.params;
    const {account} = this.state;
    removePermissionFromAccount(clusterID, account.name, permission).then(() => {
      this.setState({
        account: Object.assign({}, this.state.account, {
          permissions: _.reject(this.state.account.permissions, (p) => p.name === permission.name),
        }),
      });
      this.props.addFlashMessage({
        type: 'success',
        text: 'Removed permission from cluster account!',
      });
    }).catch(err => {
      const text = _.result(err, ['response', 'data', 'error'], 'An error occurred.');
      this.props.addFlashMessage({
        type: 'error',
        text,
      });
    });
  },

  handleRemoveAccountFromRole(role) {
    const {clusterID, accountID} = this.props.params;
    removeAccountsFromRole(clusterID, role.name, [accountID]).then(() => {
      this.setState({
        account: Object.assign({}, this.state.account, {
          roles: this.state.account.roles.filter(r => r.name !== role.name),
        }),
      });
      this.props.addFlashMessage({
        type: 'success',
        text: 'Cluster account removed from role!',
      });
    }).catch(err => {
      this.props.addFlashMessage({
        type: 'error',
        text: `An error occured. ${err.message}.`,
      });
    });
  },

  handleRemoveWebUserFromAccount(user) {
    const {clusterID} = this.props.params;
    // TODO: update this process to just include a call to
    // deleteUserClusterLinkByUserID which is currently in development
    getUserClusterLinks(clusterID).then(({data}) => {
      const clusterLinkToDelete = data.find((cl) => cl.cluster_id === clusterID && cl.user_id === user.id);
      deleteUserClusterLink(clusterID, clusterLinkToDelete.id).then(() => {
        this.setState({assignedWebUsers: this.state.assignedWebUsers.filter(u => u.id !== user.id)});

        this.props.addFlashMessage({
          type: 'success',
          text: `${user.name} removed from this cluster account`,
        });
      }).catch((err) => {
        console.error(err); // eslint-disable-line no-console
        this.props.addFlashMessage({
          type: 'error',
          text: 'Something went wrong while removing this user',
        });
      });
    });
  },

  handleAddRoleToAccount(role) {
    const {clusterID, accountID} = this.props.params;
    addAccountsToRole(clusterID, role.name, [accountID]).then(() => {
      this.setState({
        account: Object.assign({}, this.state.account, {
          roles: this.state.account.roles.concat(role),
        }),
      });
      this.props.addFlashMessage({
        type: 'success',
        text: 'Cluster account added to role!',
      });
    }).catch(err => {
      this.props.addFlashMessage({
        type: 'error',
        text: `An error occured. ${err.message}.`,
      });
    });
  },

  handleAddWebUsersToAccount(users) {
    const {clusterID, accountID} = this.props.params;
    const userIDs = users.map((u) => {
      return {
        user_id: u.id,
      };
    });

    addWebUsersToClusterAccount(clusterID, accountID, userIDs).then(() => {
      this.setState({assignedWebUsers: this.state.assignedWebUsers.concat(users)});
      this.props.addFlashMessage({
        type: 'success',
        text: `Web users added to ${accountID}`,
      });
    }).catch((err) => {
      console.error(err); // eslint-disable-line no-console
      this.props.addFlashMessage({
        type: 'error',
        text: `Something went wrong`,
      });
    });
  },

  handleDeleteAccount() {
    const {clusterID, accountID} = this.props.params;
    deleteClusterAccount(clusterID, accountID).then(() => {
      this.props.router.push(`/accounts`);
      this.props.addFlashMessage({
        type: 'success',
        text: 'Cluster account deleted!',
      });
    }).catch(err => {
      this.props.addFlashMessage({
        type: 'error',
        text: `An error occured. ${err.message}.`,
      });
    });
  },

  render() {
    const {clusterID, accountID} = this.props.params;
    const {account, databases, roles, me} = this.state;

    return (
      <ClusterAccountEditPage
        clusterID={clusterID}
        accountID={accountID}
        databases={databases}
        account={account}
        roles={roles}
        assignedWebUsers={this.state.assignedWebUsers}
        unassignedWebUsers={this.state.unassignedWebUsers}
        allPermissions={buildAllPermissions()}
        me={me}
        onAddPermission={this.handleAddPermission}
        onRemovePermission={this.handleRemovePermission}
        onUpdatePassword={this.handleUpdatePassword}
        onRemoveAccountFromRole={this.handleRemoveAccountFromRole}
        onRemoveWebUserFromAccount={this.handleRemoveWebUserFromAccount}
        onAddRoleToAccount={this.handleAddRoleToAccount}
        onAddWebUsersToAccount={this.handleAddWebUsersToAccount}
        onDeleteAccount={this.handleDeleteAccount}
      />
    );
  },
});

export default withRouter(ClusterAccountContainer);
