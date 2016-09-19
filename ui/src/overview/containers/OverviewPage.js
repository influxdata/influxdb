import React, {PropTypes} from 'react';
import NodeTable from '../components/NodeTable';
import MiscStatsPanel from '../components/MiscStatsPanel';
import ClusterStatsPanel from '../components/ClusterStatsPanel';
import FlashMessages from 'shared/components/FlashMessages';
import {showCluster} from 'shared/apis';

export const OverviewPage = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    addFlashMessage: PropTypes.func.isRequired, // Injected by the `FlashMessages` wrapper
  },

  getInitialState() {
    return {
      isFetching: true,
      // Raw output of plutonium's `/show-cluster` - includes both meta
      // and data node information.
      cluster: {},
    };
  },

  componentDidMount() {
    const {clusterID} = this.props.params;
    showCluster(clusterID).then((resp) => {
      this.setState({cluster: resp.data});
    }).catch(() => {
      this.props.addFlashMessage({
        text: 'Something went wrong! Try refreshing your browser and email support@influxdata.com if the problem persists.',
        type: 'error',
      });
    }).then(() => {
      this.setState({isFetching: false});
    });
  },

  render() {
    if (this.state.isFetching) {
      return <div className="page-spinner" />;
    }

    const {cluster} = this.state;
    const {dataNodes, params: {clusterID}} = this.props;
    const clusterPanelRefreshMs = 10000;
    const nodeTableRefreshMs = 10000;
    const miscPanelRefreshMs = 30000;

    return (
      <div className="overview">
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                Cluster Overiew
              </h1>
            </div>
          </div>
        </div>
        <div className="container-fluid">
          <div className="row">
            <ClusterStatsPanel clusterID={clusterID} refreshIntervalMs={clusterPanelRefreshMs} dataNodes={dataNodes} />
            <MiscStatsPanel clusterID={clusterID} refreshIntervalMs={miscPanelRefreshMs} dataNodes={dataNodes} />
          </div>{/* /row */}

          <div className="row">
            <NodeTable dataNodes={dataNodes} cluster={cluster} clusterID={clusterID} refreshIntervalMs={nodeTableRefreshMs}/>
          </div>
        </div>{/* /container */}
      </div>
    );
  },
});

export default FlashMessages(OverviewPage);
