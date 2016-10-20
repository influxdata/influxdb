import React, {PropTypes} from 'react';
import {withRouter} from 'react-router';
import RolePage from '../components/RolePage';
import {showDatabases} from 'src/shared/apis/metaQuery';
import showDatabasesParser from 'shared/parsing/showDatabases';
import {buildRoles, buildAllPermissions} from 'src/shared/presenters';
import {
  getRoles,
  removeAccountsFromRole,
  addAccountsToRole,
  deleteRole,
  addPermissionToRole,
  removePermissionFromRole,
} from 'src/shared/apis';
import _ from 'lodash';

export const RolePageContainer = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
      roleSlug: PropTypes.string.isRequired,
    }).isRequired,
    router: React.PropTypes.shape({
      push: React.PropTypes.func.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func,
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired),
  },

  getInitialState() {
    return {
      role: {},
      roles: [],
      databases: [],
      isFetching: true,
    };
  },

  componentDidMount() {
    const {clusterID, roleSlug} = this.props.params;
    this.getRole(clusterID, roleSlug);
  },

  componentWillReceiveProps(nextProps) {
    if (this.props.params.roleSlug !== nextProps.params.roleSlug) {
      this.setState(this.getInitialState());
      this.getRole(nextProps.params.clusterID, nextProps.params.roleSlug);
    }
  },

  getRole(clusterID, roleName) {
    this.setState({isFetching: true});
    Promise.all([
      getRoles(clusterID, roleName),
      showDatabases(this.props.dataNodes, this.props.params.clusterID),
    ]).then(([rolesResp, dbResp]) => {
      // Fetch databases for adding permissions/resources
      const {errors, databases} = showDatabasesParser(dbResp.data);
      if (errors.length) {
        this.props.addFlashMessage({
          type: 'error',
          text: `InfluxDB error: ${errors[0]}`,
        });
      }

      const roles = buildRoles(rolesResp.data.roles);
      const activeRole = roles.find(role => role.name === roleName);
      this.setState({
        role: activeRole,
        roles,
        databases,
        isFetching: false,
      });
    }).catch(err => {
      this.setState({isFetching: false});
      console.error(err.toString()); // eslint-disable-line no-console
      this.props.addFlashMessage({
        type: 'error',
        text: `Unable to fetch role! Please try refreshing the page.`,
      });
    });
  },

  handleRemoveClusterAccount(username) {
    const {clusterID, roleSlug} = this.props.params;
    removeAccountsFromRole(clusterID, roleSlug, [username]).then(() => {
      this.setState({
        role: Object.assign({}, this.state.role, {
          users: _.reject(this.state.role.users, (user) => user === username),
        }),
      });
      this.props.addFlashMessage({
        type: 'success',
        text: 'Cluster account removed from role!',
      });
    }).catch(err => {
      this.addErrorNotification(err);
    });
  },

  handleDeleteRole() {
    const {clusterID, roleSlug} = this.props.params;
    deleteRole(clusterID, roleSlug).then(() => {
      // TODO: add success notification when we're implementing them higher in the tree.
      // Right now the notification just gets swallowed when we transition to a new route.
      this.props.router.push(`/roles`);
    }).catch(err => {
      console.error(err.toString()); // eslint-disable-line no-console
      this.addErrorNotification(err);
    });
  },

  handleAddPermission(permission) {
    const {clusterID, roleSlug} = this.props.params;
    addPermissionToRole(clusterID, roleSlug, permission).then(() => {
      this.getRole(clusterID, roleSlug);
    }).then(() => {
      this.props.addFlashMessage({
        type: 'success',
        text: 'Added permission to role!',
      });
    }).catch(err => {
      this.addErrorNotification(err);
    });
  },

  handleRemovePermission(permission) {
    const {clusterID, roleSlug} = this.props.params;
    removePermissionFromRole(clusterID, roleSlug, permission).then(() => {
      this.setState({
        role: Object.assign({}, this.state.role, {
          permissions: _.reject(this.state.role.permissions, (p) => p.name === permission.name),
        }),
      });
      this.props.addFlashMessage({
        type: 'success',
        text: 'Removed permission from role!',
      });
    }).catch(err => {
      this.addErrorNotification(err);
    });
  },

  handleAddClusterAccount(clusterAccountName) {
    const {clusterID, roleSlug} = this.props.params;
    addAccountsToRole(clusterID, roleSlug, [clusterAccountName]).then(() => {
      this.getRole(clusterID, roleSlug);
    }).then(() => {
      this.props.addFlashMessage({
        type: 'success',
        text: 'Added cluster account to role!',
      });
    }).catch(err => {
      this.addErrorNotification(err);
    });
  },

  addErrorNotification(err) {
    const text = _.result(err, ['response', 'data', 'error', 'toString'], 'An error occurred.');
    this.props.addFlashMessage({
      type: 'error',
      text,
    });
  },

  render() {
    if (this.state.isFetching) {
      return <div className="page-spinner" />;
    }

    const {clusterID, roleSlug} = this.props.params;
    const {role, roles, databases} = this.state;

    return (
      <RolePage
        role={role}
        roles={roles}
        allPermissions={buildAllPermissions()}
        databases={databases}
        roleSlug={roleSlug}
        clusterID={clusterID}
        onRemoveClusterAccount={this.handleRemoveClusterAccount}
        onDeleteRole={this.handleDeleteRole}
        onAddPermission={this.handleAddPermission}
        onRemovePermission={this.handleRemovePermission}
        onAddClusterAccount={this.handleAddClusterAccount}
      />
    );
  },
});

export default withRouter(RolePageContainer);
