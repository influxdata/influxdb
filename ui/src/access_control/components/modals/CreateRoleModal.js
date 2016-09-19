import React, {PropTypes} from 'react';

const CreateRoleModal = React.createClass({
  propTypes: {
    onConfirm: PropTypes.func.isRequired,
  },

  handleSubmit(e) {
    e.preventDefault();
    if (this.roleName.value === '') {
      return;
    }

    this.props.onConfirm(this.roleName.value);
    this.roleName.value = '';
    $('#createRoleModal').modal('hide'); // eslint-disable-line no-undef
  },

  render() {
    return (
      <div className="modal fade in" id="createRoleModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Create Role</h4>
            </div>
            <form onSubmit={this.handleSubmit}>
              <div className="modal-body">
                <div className="form-grid padding-top">
                  <div className="form-group col-md-8 col-md-offset-2">
                    <label htmlFor="roleName" className="sr-only">Name this Role</label>
                    <input ref={(n) => this.roleName = n}name="roleName" type="text" className="form-control input-lg" id="roleName" placeholder="Name this Role" required={true} />
                  </div>
                </div>
              </div>
              <div className="modal-footer">
                <button type="button" className="btn btn-default" data-dismiss="modal">Cancel</button>
                <input className="btn btn-success" type="submit" value="Create" />
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },
});

export default CreateRoleModal;
