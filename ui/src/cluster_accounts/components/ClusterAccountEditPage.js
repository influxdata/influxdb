import React, {PropTypes} from 'react';
import RolePanels from 'src/shared/components/RolePanels';
import PermissionsTable from 'src/shared/components/PermissionsTable';
import UsersTable from 'shared/components/UsersTable';
import ClusterAccountDetails from '../components/ClusterAccountDetails';
import AddRoleModal from '../components/AddRoleModal';
import AddPermissionModal from 'shared/components/AddPermissionModal';
import AttachWebUsers from '../components/AttachWebUsersModal';
import RemoveAccountFromRoleModal from '../components/RemoveAccountFromRoleModal';
import RemoveWebUserModal from '../components/RemoveUserFromAccountModal';
import DeleteClusterAccountModal from '../components/DeleteClusterAccountModal';
import {Tab, TabList, TabPanels, TabPanel, Tabs} from 'shared/components/Tabs';

const {shape, string, func, arrayOf, number, bool} = PropTypes;
const TABS = ['Roles', 'Permissions', 'Account Details', 'Web Users'];

export const ClusterAccountEditPage = React.createClass({
  propTypes: {
    // All permissions to populate the "Add permission" modal
    allPermissions: arrayOf(shape({
      displayName: string.isRequired,
      name: string.isRequired,
      description: string.isRequired,
    })),
    clusterID: string.isRequired,
    accountID: string.isRequired,
    account: shape({
      name: string.isRequired,
      hash: string,
      permissions: arrayOf(shape({
        name: string.isRequired,
        displayName: string.isRequired,
        description: string.isRequired,
        resources: arrayOf(string.isRequired).isRequired,
      })).isRequired,
      roles: arrayOf(shape({
        name: string.isRequired,
        users: arrayOf(string.isRequired).isRequired,
        permissions: arrayOf(shape({
          name: string.isRequired,
          displayName: string.isRequired,
          description: string.isRequired,
          resources: arrayOf(string.isRequired).isRequired,
        })).isRequired,
      })).isRequired,
    }),
    roles: arrayOf(shape({
      name: string.isRequired,
      users: arrayOf(string.isRequired).isRequired,
      permissions: arrayOf(shape({
        name: string.isRequired,
        displayName: string.isRequired,
        description: string.isRequired,
        resources: arrayOf(string.isRequired).isRequired,
      })).isRequired,
    })),
    databases: arrayOf(string.isRequired),
    assignedWebUsers: arrayOf(shape({
      id: number.isRequired,
      name: string.isRequired,
      email: string.isRequired,
      admin: bool.isRequired,
    })),
    unassignedWebUsers: arrayOf(shape({
      id: number.isRequired,
      name: string.isRequired,
      email: string.isRequired,
      admin: bool.isRequired,
    })),
    me: shape(),
    onUpdatePassword: func.isRequired,
    onRemoveAccountFromRole: func.isRequired,
    onRemoveWebUserFromAccount: func.isRequired,
    onAddRoleToAccount: func.isRequired,
    onAddPermission: func.isRequired,
    onRemovePermission: func.isRequired,
    onAddWebUsersToAccount: func.isRequired,
    onDeleteAccount: func.isRequired,
  },

  getInitialState() {
    return {
      roleToRemove: {},
      userToRemove: {},
      activeTab: TABS[0],
    };
  },

  handleActivateTab(activeIndex) {
    this.setState({activeTab: TABS[activeIndex]});
  },

  handleRemoveAccountFromRole(role) {
    this.setState({roleToRemove: role});
  },

  handleUserToRemove(userToRemove) {
    this.setState({userToRemove});
  },

  getUnassignedRoles() {
    return this.props.roles.filter(role => {
      return !this.props.account.roles.map(r => r.name).includes(role.name);
    });
  },

  render() {
    const {clusterID, accountID, account, databases, onAddPermission, me,
      assignedWebUsers, unassignedWebUsers, onAddWebUsersToAccount, onRemovePermission, onDeleteAccount} = this.props;

    if (!account || !Object.keys(me).length) {
      return null; // TODO: 404?
    }

    return (
      <div id="user-edit-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                {accountID}&nbsp;<span className="label label-warning">Cluster Account</span>
              </h1>
            </div>
            {this.renderActions()}
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <Tabs onSelect={this.handleActivateTab}>
                <TabList>
                  {TABS.map(tab => <Tab key={tab}>{tab}</Tab>)}
                </TabList>

                <TabPanels>
                  <TabPanel>
                    <RolePanels
                      roles={account.roles}
                      clusterID={clusterID}
                      onRemoveAccountFromRole={this.handleRemoveAccountFromRole}
                    />
                  </TabPanel>
                  <TabPanel>
                    <PermissionsTable permissions={account.permissions} onRemovePermission={onRemovePermission} />
                  </TabPanel>
                  <TabPanel>
                    <ClusterAccountDetails
                      showDelete={me.cluster_links.every(cl => cl.cluster_user !== account.name)}
                      name={account.name}
                      onUpdatePassword={this.props.onUpdatePassword}
                    />
                  </TabPanel>
                  <TabPanel>
                    <div className="panel panel-default">
                      <div className="panel-body">
                        <UsersTable
                          onUserToDelete={this.handleUserToRemove}
                          activeCluster={clusterID}
                          users={assignedWebUsers}
                          me={me}
                          deleteText="Unlink" />
                      </div>
                    </div>
                  </TabPanel>
                </TabPanels>
              </Tabs>
            </div>
          </div>
        </div>
        <AddPermissionModal
          activeCluster={clusterID}
          permissions={this.props.allPermissions}
          databases={databases}
          onAddPermission={onAddPermission}
        />
        <RemoveAccountFromRoleModal
          roleName={this.state.roleToRemove.name}
          onConfirm={() => this.props.onRemoveAccountFromRole(this.state.roleToRemove)}
        />
        <AddRoleModal
          account={account}
          roles={this.getUnassignedRoles()}
          onAddRoleToAccount={this.props.onAddRoleToAccount}
        />
        <RemoveWebUserModal
          account={accountID}
          onRemoveWebUser={() => this.props.onRemoveWebUserFromAccount(this.state.userToRemove)}
          user={this.state.userToRemove.name}
        />
        <AttachWebUsers
          account={accountID}
          users={unassignedWebUsers}
          onConfirm={onAddWebUsersToAccount}
        />
        <DeleteClusterAccountModal
          account={account}
          webUsers={assignedWebUsers}
          onConfirm={onDeleteAccount}
        />
      </div>
    );
  },

  renderActions() {
    const {activeTab} = this.state;
    return (
      <div className="enterprise-header__right">
        {activeTab === 'Roles' ? (
          <button
            className="btn btn-sm btn-primary"
            data-toggle="modal"
            data-target="#addRoleModal">
            Add to Role
          </button>
        ) : null}
        {activeTab === 'Permissions' ? (
          <button
            className="btn btn-sm btn-primary"
            data-toggle="modal"
            data-target="#addPermissionModal">
            Add Permissions
          </button>
        ) : null}
        {activeTab === 'Web Users' ? (
          <button
            className="btn btn-sm btn-primary"
            data-toggle="modal"
            data-target="#addWebUsers">
            Link Web Users
          </button>
        ) : null}
      </div>
    );
  },
});

export default ClusterAccountEditPage;
