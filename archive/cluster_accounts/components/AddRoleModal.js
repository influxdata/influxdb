import React, {PropTypes} from 'react';

const {shape, string, arrayOf, func} = PropTypes;

const AddRoleModal = React.createClass({
  propTypes: {
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
    onAddRoleToAccount: func.isRequired,
  },

  getInitialState() {
    return {
      selectedRole: this.props.roles[0],
    };
  },

  handleChangeRole(e) {
    this.setState({selectedRole: this.props.roles.find((role) => role.name === e.target.value)});
  },

  handleSubmit(e) {
    e.preventDefault();
    $('#addRoleModal').modal('hide'); // eslint-disable-line no-undef
    this.props.onAddRoleToAccount(this.state.selectedRole);
  },

  render() {
    const {account, roles} = this.props;
    const {selectedRole} = this.state;

    if (!roles.length) {
      return (
        <div className="modal fade" id="addRoleModal" tabIndex="-1" role="dialog">
          <div className="modal-dialog modal-lg">
            <div className="modal-content">
              <div className="modal-header">
                <h4>This cluster account already belongs to all roles.</h4>
              </div>
            </div>
          </div>
        </div>
      );
    }

    return (
      <div className="modal fade" id="addRoleModal" tabIndex="-1" role="dialog">
        <form onSubmit={this.handleSubmit} className="form">
          <div className="modal-dialog modal-lg">
            <div className="modal-content">
              <div className="modal-header">
                <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                  <span aria-hidden="true">×</span>
                </button>
                <h4 className="modal-title">Add <strong>{account.name}</strong> to a new Role</h4>
              </div>
              <div className="modal-body">
                <div className="row">
                  <div className="col-xs-6 col-xs-offset-3">
                    <label htmlFor="roles-select">Available Roles</label>
                    <select id="roles-select" onChange={this.handleChangeRole} value={selectedRole.name} className="form-control input-lg" name="roleName">
                      {roles.map((role) => {
                        return <option key={role.name} >{role.name}</option>;
                      })}
                    </select>
                    <br/>
                  </div>
                  <div className="col-xs-10 col-xs-offset-1">
                    <h4>Permissions</h4>
                    <div className="well well-white">
                      {this.renderRoleTable()}
                    </div>
                  </div>
                </div>
              </div>
              <div className="modal-footer">
                <button className="btn btn-default" data-dismiss="modal">Cancel</button>
                <input className="btn btn-success" type="submit" value="Add to Role" />
              </div>
            </div>
          </div>
        </form>
      </div>
    );
  },

  renderRoleTable() {
    return (
      <table className="table permissions-table">
        <tbody>
          {this.renderPermissions()}
        </tbody>
      </table>
    );
  },

  renderPermissions() {
    const role = this.state.selectedRole;

    if (!role.permissions.length) {
      return (
        <tr className="role-row">
          <td>
            <div className="generic-empty-state">
              <span className="icon alert-triangle"></span>
              <h4>This Role has no Permissions</h4>
            </div>
          </td>
        </tr>
      );
    }

    return role.permissions.map((p) => {
      return (
        <tr key={p.name} className="role-row">
          <td>{p.displayName}</td>
          <td>
            {p.resources.map((resource, i) => (
              <div key={i} className="pill">{resource === '' ? 'All Databases' : resource}</div>
            ))}
          </td>
        </tr>
      );
    });
  },
});

export default AddRoleModal;
