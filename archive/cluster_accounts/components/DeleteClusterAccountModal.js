import React, {PropTypes} from 'react';

const DeleteClusterAccountModal = React.createClass({
  propTypes: {
    onConfirm: PropTypes.func.isRequired,
    account: PropTypes.shape({
      name: PropTypes.string,
    }),
    webUsers: PropTypes.arrayOf(PropTypes.shape()), // TODO
  },

  handleConfirm() {
    this.props.onConfirm();
  },

  render() {
    return (
      <div className="modal fade" id="deleteClusterAccountModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">{`Are you sure you want to delete ${this.props.account && this.props.account.name}?`}</h4>
            </div>
            {this.props.webUsers.length ? (
              <div className="modal-body">
                <h5>
                  The following web users are associated with this cluster account will need to be reassigned
                  to another cluster account to continue using many of EnterpriseWeb's features:
                </h5>
                <ul>
                  {this.props.webUsers.map(webUser => {
                    return <li key={webUser.id}>{webUser.email}</li>;
                  })}
                </ul>
              </div>
            ) : null}
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button className="btn btn-danger js-delete-users" onClick={this.handleConfirm} type="button" data-dismiss="modal">Confirm</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default DeleteClusterAccountModal;
