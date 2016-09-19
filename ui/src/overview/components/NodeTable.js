import React, {PropTypes} from 'react';
import _ from 'lodash';

import NodeTableRow from './NodeTableRow';

const NodeTable = React.createClass({
  propTypes: {
    refreshIntervalMs: PropTypes.number.isRequired,
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    cluster: PropTypes.shape({
      data: PropTypes.arrayOf(PropTypes.shape({})).isRequired,
      meta: PropTypes.arrayOf(PropTypes.shape({})).isRequired,
    }),
    clusterID: PropTypes.string.isRequired,
  },

  render() {
    const {cluster, clusterID, dataNodes, refreshIntervalMs} = this.props;
    const data = _.map(cluster.data, (node) => {
      // The nodeID tag is for data nodes is currently the TCP address, but is fickle and
      // possibly subject to change.
      return {addr: node.httpAddr, type: 'data', nodeID: node.tcpAddr};
    });
    const meta = _.map(cluster.meta, (node) => {
      return {addr: node.addr, type: 'meta'};
    });

    return (
      <div>
        <div className="col-lg-8">
          <div className="panel panel-minimal">
            <div className="panel-heading">
              <h2 className="panel-title">Data</h2>
            </div>
            <div className="panel-body">
              <div className="table-responsive">
                <table className="table v-center">
                  <thead>
                    <tr>
                      <th>Node</th>
                      <th>Disk Used</th>
                      <th>Active Queries</th>
                      <th>Active Writes</th>
                    </tr>
                  </thead>
                  <tbody>
                    {data.map((n, i) => <NodeTableRow key={i} node={n} clusterID={clusterID} dataNodes={dataNodes} refreshIntervalMs={refreshIntervalMs}/>)}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
        <div className="col-lg-4">
          <div className="panel panel-minimal">
            <div className="panel-heading">
              <h2 className="panel-title">Meta</h2>
            </div>
            <div className="panel-body">
              <div className="">
                <table className="table v-center js-node-table">
                  <thead>
                    <tr>
                      <th>Node</th>
                    </tr>
                  </thead>
                  <tbody>
                    {meta.map((n, i) => <NodeTableRow key={i} node={n} clusterID={clusterID} dataNodes={dataNodes} refreshIntervalMs={refreshIntervalMs}/>)}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default NodeTable;
