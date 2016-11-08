import React, {PropTypes} from 'react';

const DeleteUserModal = React.createClass({
  propTypes: {
    onConfirm: PropTypes.func.isRequired,
    user: PropTypes.shape({
      name: PropTypes.string,
    }),
  },

  handleConfirm() {
    this.props.onConfirm(this.props.user);
  },

  render() {
    return (
      <div className="modal fade" id="deleteUserModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">{this.props.user ? `Are you sure you want to delete ${this.props.user.name}?` : 'Are you sure?'}</h4>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button className="btn btn-danger js-delete-users" onClick={this.handleConfirm} type="button" data-dismiss="modal">Delete</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default DeleteUserModal;
