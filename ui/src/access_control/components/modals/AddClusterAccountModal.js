import React, {PropTypes} from 'react';
import AddClusterAccounts from 'src/shared/components/AddClusterAccounts';
import {getClusterAccounts} from 'src/shared/apis';

const {arrayOf, func, string, shape} = PropTypes;

// Allows a user to add a cluster account to a role. Very similar to other features
// (e.g. adding cluster accounts to a web user), the main difference being that
// we'll only give users the option to select users from the active cluster instead of
// from all clusters.
const AddClusterAccountModal = React.createClass({
  propTypes: {
    clusterID: string.isRequired,
    onAddClusterAccount: func.isRequired,
    // Cluster accounts that already belong to a role so we can filter
    // the list of available options.
    roleClusterAccounts: arrayOf(string),
    role: shape({
      name: PropTypes.string,
    }),
  },

  getDefaultProps() {
    return {roleClusterAccounts: []};
  },

  getInitialState() {
    return {
      selectedAccount: null,
      clusterAccounts: [],
      error: null,
      isFetching: true,
    };
  },

  componentDidMount() {
    getClusterAccounts(this.props.clusterID).then((resp) => {
      this.setState({clusterAccounts: resp.data.users});
    }).catch(() => {
      this.setState({error: 'An error occured.'});
    }).then(() => {
      this.setState({isFetching: false});
    });
  },

  handleSubmit(e) {
    e.preventDefault();
    this.props.onAddClusterAccount(this.state.selectedAccount);
    $('#addClusterAccountModal').modal('hide'); // eslint-disable-line no-undef
  },

  handleSelectClusterAccount({accountName}) {
    this.setState({
      selectedAccount: accountName,
    });
  },

  render() {
    if (this.state.isFetching) {
      return null;
    }
    const {role} = this.props;

    // Temporary hack while https://github.com/influxdata/enterprise/issues/948 is resolved.
    // We want to use the /api/int/v1/clusters endpoint and just pick the
    // Essentially we're taking the raw output from /user and morphing whatthe `AddClusterAccounts`
    // modal expects (a cluster with fields defined by the enterprise web database)
    const availableClusterAccounts = this.state.clusterAccounts.filter((account) => {
      return !this.props.roleClusterAccounts.includes(account.name);
    });
    const cluster = {
      id: 0, // Only used as a `key` prop
      cluster_users: availableClusterAccounts,
      cluster_id: this.props.clusterID,
    };

    return (
      <div className="modal fade in" id="addClusterAccountModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Add Cluster Account to <strong>{role.name}</strong></h4>
            </div>
            <form onSubmit={this.handleSubmit}>
              <div className="modal-body">
                <div className="alert alert-info">
                  <span className="icon star"></span>
                  <p><strong>NOTE:</strong> Cluster Accounts added to a Role inherit all the permissions associated with that Role.</p>
                </div>
                <AddClusterAccounts
                  clusters={[cluster]}
                  onSelectClusterAccount={this.handleSelectClusterAccount}
                />
              </div>
              <div className="modal-footer">
                <button className="btn btn-default" data-dismiss="modal">Cancel</button>
                <input disabled={!this.state.selectedAccount} className="btn btn-success" type="submit" value="Confirm" />
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },
});

export default AddClusterAccountModal;
