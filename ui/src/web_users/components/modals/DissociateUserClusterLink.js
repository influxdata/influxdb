import React, {PropTypes} from 'react';

const {func, string, number, shape} = PropTypes;
const DissociateUserClusterLink = React.createClass({
  propTypes: {
    handleDissociate: func.isRequired,
    clusterLink: shape({
      id: number,
      name: string,
      cluster_user: string,
    }).isRequired,
    user: shape({
      name: string,
    }).isRequired,
  },

  onConfirmDissociate() {
    this.props.handleDissociate(this.props.clusterLink);
  },

  render() {
    const {clusterLink, user} = this.props;
    return (
      <div className="modal fade" id="dissociateLink" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <h4 className="modal-title">
                Are you sure you want to unlink <strong>{clusterLink.cluster_user}</strong> from <strong>{user.name}</strong>?
              </h4>
            </div>
            <div className="modal-footer">
              <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
              <button className="btn btn-danger js-delete-users" onClick={this.onConfirmDissociate} type="button" data-dismiss="modal">Dissociate</button>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default DissociateUserClusterLink;
