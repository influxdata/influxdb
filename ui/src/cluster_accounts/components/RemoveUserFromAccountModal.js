import React, {PropTypes} from 'react';

const {string, func} = PropTypes;

const RemoveWebUserModal = React.createClass({
  propTypes: {
    user: string,
    onRemoveWebUser: func.isRequired,
    account: string.isRequired,
  },

  handleConfirm() {
    $('#deleteUsersModal').modal('hide'); // eslint-disable-line no-undef
    this.props.onRemoveWebUser();
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
              <h4 className="modal-title">
                Are you sure you want to remove
                <strong> {this.props.user} </strong> from
                <strong> {this.props.account} </strong> ?
              </h4>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button onClick={this.handleConfirm} className="btn btn-danger">Remove</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default RemoveWebUserModal;
