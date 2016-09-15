import React, {PropTypes} from 'react';
import {Tab, Tabs, TabPanel, TabPanels, TabList} from 'src/shared/components/Tabs';
import RoleHeader from '../components/RoleHeader';
import RoleClusterAccounts from '../components/RoleClusterAccounts';
import PermissionsTable from 'src/shared/components/PermissionsTable';
import AddPermissionModal from 'src/shared/components/AddPermissionModal';
import AddClusterAccountModal from '../components/modals/AddClusterAccountModal';
import DeleteRoleModal from '../components/modals/DeleteRoleModal';

const {arrayOf, string, shape, func} = PropTypes;
const TABS = ['Permissions', 'Cluster Accounts'];

const RolePage = React.createClass({
  propTypes: {
    // All permissions to populate the "Add permission" modal
    allPermissions: arrayOf(shape({
      displayName: string.isRequired,
      name: string.isRequired,
      description: string.isRequired,
    })),

    // All roles to populate the navigation dropdown
    roles: arrayOf(shape({})),
    role: shape({
      id: string,
      name: string.isRequired,
      permissions: arrayOf(shape({
        displayName: string.isRequired,
        name: string.isRequired,
        description: string.isRequired,
        resources: arrayOf(string.isRequired).isRequired,
      })),
    }),
    databases: arrayOf(string.isRequired),
    clusterID: string.isRequired,
    roleSlug: string.isRequired,
    onRemoveClusterAccount: func.isRequired,
    onDeleteRole: func.isRequired,
    onAddPermission: func.isRequired,
    onAddClusterAccount: func.isRequired,
    onRemovePermission: func.isRequired,
  },

  getInitialState() {
    return {activeTab: TABS[0]};
  },

  handleActivateTab(activeIndex) {
    this.setState({activeTab: TABS[activeIndex]});
  },

  render() {
    const {role, roles, allPermissions, databases, clusterID,
           onDeleteRole, onRemoveClusterAccount, onAddPermission, onRemovePermission, onAddClusterAccount} = this.props;

    return (
      <div id="role-edit-page" className="js-role-edit">
        <RoleHeader
          roles={roles}
          selectedRole={role}
          clusterID={clusterID}
          activeTab={this.state.activeTab}
        />
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <Tabs onSelect={this.handleActivateTab}>
                <TabList>
                  <Tab>{TABS[0]}</Tab>
                  <Tab>{TABS[1]}</Tab>
                </TabList>
                <TabPanels>
                  <TabPanel>
                    <PermissionsTable
                      permissions={role.permissions}
                      showAddResource={true}
                      onRemovePermission={onRemovePermission}
                    />
                  </TabPanel>
                  <TabPanel>
                    <RoleClusterAccounts
                      clusterID={clusterID}
                      users={role.users}
                      onRemoveClusterAccount={onRemoveClusterAccount}
                    />
                  </TabPanel>
                </TabPanels>
              </Tabs>
            </div>
          </div>
        </div>
        <DeleteRoleModal onDeleteRole={onDeleteRole} roleName={role.name} />
        <AddPermissionModal
          permissions={allPermissions}
          activeCluster={clusterID}
          databases={databases}
          onAddPermission={onAddPermission}
        />
        <AddClusterAccountModal
          clusterID={clusterID}
          onAddClusterAccount={onAddClusterAccount}
          roleClusterAccounts={role.users}
          role={role}
        />
      </div>
    );
  },
});

export default RolePage;
