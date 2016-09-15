import React, {PropTypes} from 'react';
import {formatBytes} from 'utils/formatting';
import MiniGraph from 'shared/components/MiniGraph';
import AutoRefresh from 'shared/components/AutoRefresh';
import {nodeDiskUsage} from 'shared/apis/stats';
import {diskBytesFromShard} from 'shared/parsing/diskBytes';
const RefreshingMiniGraph = AutoRefresh(MiniGraph);
import {OVERVIEW_TIME_RANGE, OVERVIEW_INTERVAL} from '../constants';

const {string, shape, number} = PropTypes;
const NodeTableRow = React.createClass({
  propTypes: {
    refreshIntervalMs: number.isRequired,
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    clusterID: string.isRequired,
    node: shape({
      addr: string.isRequired,
      type: string.isRequired,
    }),
  },

  getInitialState() {
    return {
      diskUsed: 0,
    };
  },

  componentDidMount() {
    if (this.props.node.type !== 'data') {
      return;
    }

    const {node, dataNodes, clusterID} = this.props;

    nodeDiskUsage(dataNodes, clusterID, node.nodeID).then((resp) => {
      this.setState({
        diskUsed: diskBytesFromShard(resp.data).bytes,
      });
    });
  },

  render() {
    const {node, dataNodes, clusterID, refreshIntervalMs} = this.props;
    const diskUsed = formatBytes(this.state.diskUsed);

    const activeQueries = `SELECT NON_NEGATIVE_DERIVATIVE(max(queryReq)) FROM httpd WHERE time > now() - ${OVERVIEW_TIME_RANGE} AND nodeID='${node.nodeID}' AND clusterID='${clusterID}' GROUP by time(${OVERVIEW_INTERVAL})`;
    const activeWrites = `SELECT NON_NEGATIVE_DERIVATIVE(max(req)) FROM "write" WHERE time > now() - ${OVERVIEW_TIME_RANGE} AND nodeID='${node.nodeID}' AND clusterID='${clusterID}' GROUP BY time(${OVERVIEW_INTERVAL})`;

    if (node.type !== 'data') {
      return (
        <tr className="node">
          <td>{node.addr}</td>
        </tr>
      );
    }

    return (
      <tr className="node">
        <td>{node.addr}</td>
        <td>{diskUsed}</td>
        <td>
          <RefreshingMiniGraph clusterID={clusterID} queries={[{host: dataNodes, database: '_internal', text: activeQueries}]} autoRefresh={refreshIntervalMs}>
            <p className="cluster-stat-empty"><span className="icon cubo"></span> Waiting for stats...</p>
          </RefreshingMiniGraph>
        </td>
        <td>
          <RefreshingMiniGraph clusterID={clusterID} queries={[{host: dataNodes, database: '_internal', text: activeWrites}]} autoRefresh={refreshIntervalMs}>
            <p className="cluster-stat-empty"><span className="icon cubo"></span> Waiting for  stats..</p>
          </RefreshingMiniGraph>
        </td>
      </tr>
    );
  },
});

export default NodeTableRow;
