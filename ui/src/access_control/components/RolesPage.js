import React, {PropTypes} from 'react';
import CreateRoleModal from './modals/CreateRoleModal';
import RolePanels from 'src/shared/components/RolePanels';

const RolesPage = React.createClass({
  propTypes: {
    roles: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string,
      users: PropTypes.arrayOf(PropTypes.string),
      permissions: PropTypes.arrayOf(PropTypes.shape({
        name: PropTypes.string.isRequired,
        displayName: PropTypes.string.isRequired,
        description: PropTypes.string.isRequired,
        resources: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
      })),
    })).isRequired,
    onCreateRole: PropTypes.func.isRequired,
    clusterID: PropTypes.string.isRequired,
  },

  handleCreateRole(roleName) {
    this.props.onCreateRole(roleName);
  },

  render() {
    return (
      <div id="role-index-page">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>Access Control</h1>
            </div>
            <div className="enterprise-header__right">
              <button className="btn btn-sm btn-primary" data-toggle="modal" data-target="#createRoleModal">Create Role</button>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <h3 className="deluxe fake-panel-title match-search">All Roles</h3>
              <div className="panel-group sub-page roles" id="role-page" role="tablist">
                <RolePanels roles={this.props.roles} clusterID={this.props.clusterID} showUserCount={true} />
              </div>
            </div>
          </div>
        </div>
        <CreateRoleModal onConfirm={this.handleCreateRole} />
      </div>
    );
  },
});

export default RolesPage;
