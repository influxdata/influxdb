import React, {PropTypes} from 'react';

const {arrayOf, number, shape, func, string} = PropTypes;

const AddClusterAccounts = React.createClass({
  propTypes: {
    clusters: arrayOf(shape({
      id: number.isRequired,
      cluster_users: arrayOf(shape({
        name: string.isRequired,
      })),
      dipslay_name: string,
      cluster_id: string.isRequired,
    })).isRequired,
    onSelectClusterAccount: func.isRequired,
    headerText: string,
  },

  getDefaultProps() {
    return {
      headerText: 'Pair With Cluster Accounts',
    };
  },

  handleSelectClusterAccount(e, clusterID) {
    this.props.onSelectClusterAccount({
      clusterID,
      accountName: e.target.value,
    });
  },

  render() {
    return (
      <div>
        {
          this.props.clusters.map((cluster, i) => {
            return (
              <div key={i} className="form-grid">
                <div className="form-group col-sm-6">
                  {i === 0 ? <label>Cluster</label> : null}
                  <div className="form-control-static">
                    {cluster.display_name || cluster.cluster_id}
                  </div>
                </div>
                <div className="form-group col-sm-6">
                  {i === 0 ? <label>Account</label> : null}
                  {this.renderClusterUsers(cluster)}
                </div>
              </div>
            );
          })
        }
      </div>
    );
  },

  renderClusterUsers(cluster) {
    if (!cluster.cluster_users) {
      return (
        <select disabled={true} defaultValue="No cluster accounts" className="form-control" id="cluster-account">
          <option>No cluster accounts</option>
        </select>
      );
    }

    return (
      <select onChange={(e) => this.handleSelectClusterAccount(e, cluster.cluster_id)} className="form-control">
        <option value="">No Association</option>
        {
          cluster.cluster_users.map((cu) => {
            return <option value={cu.name} key={cu.name}>{cu.name}</option>;
          })
        }
      </select>
    );
  },
});

export default AddClusterAccounts;
