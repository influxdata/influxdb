import React, {PropTypes} from 'react';
import {Link} from 'react-router';

const RoleHeader = React.createClass({
  propTypes: {
    selectedRole: PropTypes.shape(),
    roles: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string,
    })).isRequired,
    clusterID: PropTypes.string.isRequired,
    activeTab: PropTypes.string,
  },

  getDefaultProps() {
    return {
      selectedRole: '',
    };
  },

  render() {
    return (
      <div className="enterprise-header">
        <div className="enterprise-header__container">
          <div className="enterprise-header__left">
            <div className="dropdown minimal-dropdown">
              <button className="dropdown-toggle" type="button" id="roleSelection" data-toggle="dropdown">
                <span className="button-text">{this.props.selectedRole.name}</span>
                <span className="caret"></span>
              </button>
              <ul className="dropdown-menu" aria-labelledby="dropdownMenu1">
                {this.props.roles.map((role) => (
                  <li key={role.name}>
                    <Link to={`/clusters/${this.props.clusterID}/roles/${encodeURIComponent(role.name)}`} className="role-option">
                      {role.name}
                    </Link>
                  </li>
                ))}
                <li role="separator" className="divider"></li>
                <li>
                  <Link to={`/clusters/${this.props.clusterID}/roles`} className="role-option">
                    All Roles
                  </Link>
                </li>
              </ul>
            </div>
          </div>
          <div className="enterprise-header__right">
            <button className="btn btn-sm btn-default" data-toggle="modal" data-target="#deleteRoleModal">Delete Role</button>
            {this.props.activeTab === 'Permissions' ? (
              <button
                className="btn btn-sm btn-primary"
                data-toggle="modal"
                data-target="#addPermissionModal"
              >
              Add Permission
              </button>
            ) : null}
            {this.props.activeTab === 'Cluster Accounts' ? (
              <button
                className="btn btn-sm btn-primary"
                data-toggle="modal"
                data-target="#addClusterAccountModal"
                >
                Add Cluster Account
              </button>
            ) : null}
          </div>
        </div>
      </div>
    );
  },
});

export default RoleHeader;
