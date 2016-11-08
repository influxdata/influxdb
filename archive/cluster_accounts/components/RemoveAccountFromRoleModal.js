import React, {PropTypes} from 'react';

const RemoveAccountFromRoleModal = React.createClass({
  propTypes: {
    roleName: PropTypes.string,
    onConfirm: PropTypes.func.isRequired,
  },

  handleConfirm() {
    $('#removeAccountFromRoleModal').modal('hide'); // eslint-disable-line no-undef
    this.props.onConfirm();
  },

  render() {
    return (
      <div className="modal fade" id="removeAccountFromRoleModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">
                Are you sure you want to remove <strong>{this.props.roleName}</strong> from this cluster account?
              </h4>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button onClick={this.handleConfirm} className="btn btn-danger" value="Remove">Remove</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default RemoveAccountFromRoleModal;
