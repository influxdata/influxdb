import React, {PropTypes} from 'react';
import AddClusterAccounts from 'src/shared/components/AddClusterAccounts';
import {getClusters} from 'shared/apis';

const {func, shape, string} = PropTypes;
const AddClusterLinksModal = React.createClass({
  propTypes: {
    user: shape({name: string.isRequired}).isRequired,
    onAddClusterAccount: func.isRequired,
    onCreateClusterLinks: func.isRequired,
  },

  getInitialState() {
    return {
      clusters: [],
      clusterLinks: {},
    };
  },

  componentDidMount() {
    getClusters().then(({data: clusters}) => {
      this.setState({clusters});
    });
  },

  handleSelectClusterAccount({clusterID, accountName}) {
    const clusterLinks = Object.assign({}, this.state.clusterLinks, {
      [clusterID]: accountName,
    });
    this.setState({clusterLinks});
    this.props.onAddClusterAccount(clusterLinks);
  },

  handleSubmit(e) {
    e.preventDefault();
    $('#addClusterAccountModal').modal('hide'); // eslint-disable-line no-undef
    this.props.onCreateClusterLinks();
  },

  render() {
    return (
      <div className="modal fade in" id="addClusterAccountModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Add Cluster Accounts to <strong>{this.props.user.name}</strong></h4>
            </div>
            <form onSubmit={this.handleSubmit}>
              <div className="modal-body">
                <div className="alert alert-info">
                  <span className="icon star"></span>
                  <p><strong>NOTE:</strong> Pairing a Web User with a Cluster Account causes the Web User to inherit all the Permissions and Roles associated with the Cluster Account.</p>
                </div>
                <AddClusterAccounts
                  clusters={this.state.clusters}
                  onSelectClusterAccount={this.handleSelectClusterAccount}
                  headerText={''}
                />
              </div>
              <div className="modal-footer">
                <button className="btn btn-default" data-dismiss="modal">Cancel</button>
                <input disabled={!Object.keys(this.state.clusterLinks).length} className="btn btn-success" type="submit" value="Confirm" />
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },
});

export default AddClusterLinksModal;
