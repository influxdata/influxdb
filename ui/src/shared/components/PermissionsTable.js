import React, {PropTypes} from 'react';
import {permissionShape} from 'utils/propTypes';

const PermissionsTable = React.createClass({
  propTypes: {
    permissions: PropTypes.arrayOf(permissionShape).isRequired,
    showAddResource: PropTypes.bool,
    onRemovePermission: PropTypes.func,
  },

  getDefaultProps() {
    return {
      permissions: [],
      showAddResource: false,
    };
  },

  handleAddResourceClick() {
    // TODO
  },

  handleRemovePermission(permission) {
    this.props.onRemovePermission(permission);
  },

  render() {
    if (!this.props.permissions.length) {
      return (
        <div className="generic-empty-state">
          <span className="icon alert-triangle"></span>
          <h4>This Role has no Permissions</h4>
        </div>
      );
    }

    return (
      <div className="panel-body">
        <table className="table permissions-table">
          <tbody>
            {this.props.permissions.map((p) => (
              <tr key={p.name}>
                <td>{p.displayName}</td>
                <td>
                  {p.resources.map((resource, i) => <div key={i} className="pill">{resource === '' ? 'All Databases' : resource}</div>)}
                  {this.props.showAddResource ? (
                    <div onClick={this.handleAddResourceClick} className="pill-add" data-toggle="modal" data-target="#addPermissionModal">
                      <span className="icon plus"></span>
                    </div>
                  ) : null}
                </td>
                {this.props.onRemovePermission ? (
                  <td className="remove-permission">
                    <button
                      onClick={() => this.handleRemovePermission(p)}
                      type="button"
                      className="btn btn-sm btn-link-danger">
                      Remove
                    </button>
                  </td>
                ) : null}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  },
});

export default PermissionsTable;
