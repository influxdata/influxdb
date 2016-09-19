import React, {PropTypes} from 'react';

const CLUSTER_WIDE_PERMISSIONS = ["CreateDatabase", "AddRemoveNode", "ManageShard", "DropDatabase", "CopyShard", "Rebalance"];

const AddPermissionModal = React.createClass({
  propTypes: {
    activeCluster: PropTypes.string.isRequired,
    permissions: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string.isRequired,
      displayName: PropTypes.string.isRequired,
      description: PropTypes.string.isRequired,
    })),
    databases: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    onAddPermission: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      selectedPermission: null,
      selectedDatabase: '',
    };
  },

  handlePermissionClick(permission) {
    this.setState({
      selectedPermission: permission,
      selectedDatabase: '',
    });
  },

  handleDatabaseChange(e) {
    this.setState({selectedDatabase: e.target.value});
  },

  handleSubmit(e) {
    e.preventDefault();
    this.props.onAddPermission({
      name: this.state.selectedPermission,
      resources: [this.state.selectedDatabase],
    });
    $('#addPermissionModal').modal('hide'); // eslint-disable-line no-undef
  },

  render() {
    const {permissions} = this.props;

    return (
      <div className="modal fade" id="addPermissionModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Select a Permission to Add</h4>
            </div>
            <form onSubmit={this.handleSubmit}>
              <div className="modal-body">
                <div className="well permission-list">
                  <ul>
                    {permissions.map((perm) => {
                      return (
                        <li key={perm.name}>
                          <input onClick={() => this.handlePermissionClick(perm.name)} type="radio" name="permissionName" value={`${perm.name}`} id={`permission-${perm.name}`}></input>
                          <label htmlFor={`permission-${perm.name}`}>
                            {perm.displayName}
                            <br/>
                            <span className="permission-description">{perm.description}</span>
                          </label>
                        </li>
                      );
                    })}
                  </ul>
                </div>
                {this.renderOptions()}
              </div>
              {this.renderFooter()}
            </form>
          </div>
        </div>
      </div>
    );
  },

  renderFooter() {
    return (
      <div className="modal-footer">
        <button className="btn btn-default" data-dismiss="modal">Cancel</button>
        <input disabled={!this.state.selectedPermission} className="btn btn-success" type="submit" value="Add Permission"></input>
      </div>
    );
  },

  renderOptions() {
    return (
      <div>
        {this.state.selectedPermission ? this.renderDatabases() : null}
      </div>
    );
  },

  renderDatabases() {
    const isClusterWide = CLUSTER_WIDE_PERMISSIONS.includes(this.state.selectedPermission);
    if (!this.props.databases.length || isClusterWide) {
      return null;
    }

    return (
      <div>
        <div className="form-grid">
          <div className="form-group col-md-12">
            <label htmlFor="#permissions-database">Limit Permission to...</label>
            <select onChange={this.handleDatabaseChange} className="form-control" name="database" id="permissions-database">
              <option value={''}>All Databases</option>
              {this.props.databases.map((databaseName, i) => <option key={i}>{databaseName}</option>)}
            </select>
          </div>
        </div>
      </div>
    );
  },
});

export default AddPermissionModal;
