import React, {PropTypes} from 'react';

const {func, string, number, shape} = PropTypes;
const DeleteUsersModal = React.createClass({
  propTypes: {
    handleConfirmDeleteUser: func.isRequired,
    userToDelete: shape({
      id: number.isRequired,
      name: string.isRequired,
    }).isRequired,
  },

  onConfirmDeleteUser() {
    this.props.handleConfirmDeleteUser(this.props.userToDelete.id);
  },

  render() {
    return (
      <div className="modal fade" id="deleteUsersModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">{`Are you sure you want to delete ${this.props.userToDelete.name}?`}</h4>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button className="btn btn-danger js-delete-users" onClick={this.onConfirmDeleteUser} type="button" data-dismiss="modal">Delete</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default DeleteUsersModal;
