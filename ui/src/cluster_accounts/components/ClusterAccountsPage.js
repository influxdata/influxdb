import React, {PropTypes} from 'react';
import PageHeader from '../components/PageHeader';
import ClusterAccountsTable from '../components/ClusterAccountsTable';

const ClusterAccountsPage = React.createClass({
  propTypes: {
    clusterID: PropTypes.string.isRequired,
    users: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.string,
      roles: PropTypes.arrayOf(PropTypes.shape({
        name: PropTypes.string.isRequired,
      })),
    })),
    roles: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.shape,
    })),
    onDeleteAccount: PropTypes.func.isRequired,
    onCreateAccount: PropTypes.func.isRequired,
    me: PropTypes.shape(),
  },

  render() {
    const {clusterID, users, roles, onCreateAccount, me} = this.props;

    return (
      <div id="cluster-accounts-page" data-cluster-id={clusterID}>
        <PageHeader
          roles={roles}
          activeCluster={clusterID}
          onCreateAccount={onCreateAccount} />
        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <ClusterAccountsTable
                users={users}
                clusterID={clusterID}
                onDeleteAccount={this.props.onDeleteAccount}
                me={me}
              />
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default ClusterAccountsPage;
