import React, {PropTypes} from 'react';

const {string, func} = PropTypes;

const DeleteRoleModal = React.createClass({
  propTypes: {
    roleName: string.isRequired,
    onDeleteRole: func.isRequired,
  },

  handleConfirm() {
    $('#deleteRoleModal').modal('hide'); // eslint-disable-line no-undef
    this.props.onDeleteRole();
  },

  render() {
    return (
      <div className="modal fade" id="deleteRoleModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">
                Are you sure you want to delete <strong>{this.props.roleName}</strong>?
              </h4>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button onClick={this.handleConfirm} className="btn btn-danger" value="Delete">Delete</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default DeleteRoleModal;
