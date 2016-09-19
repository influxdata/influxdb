import React, {PropTypes} from 'react';
import MiniGraph from 'shared/components/MiniGraph';
import AutoRefresh from 'shared/components/AutoRefresh';
const RefreshingMiniGraph = AutoRefresh(MiniGraph);
import {OVERVIEW_TIME_RANGE, OVERVIEW_INTERVAL} from '../constants';

const ClusterStatsPanel = React.createClass({
  propTypes: {
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    clusterID: PropTypes.string.isRequired,
    refreshIntervalMs: PropTypes.number.isRequired,
  },
  render() {
    const {dataNodes, refreshIntervalMs} = this.props;

    // For active writes/queries, we GROUP BY nodeID because we want to grab all of the
    // values for each node and sum the results.
    const queries = [
      {
        queryDescription: "Active Queries",
        host: dataNodes,
        database: '_internal',
        text: `SELECT NON_NEGATIVE_DERIVATIVE(MAX(queryReq)) FROM httpd WHERE time > now() - ${OVERVIEW_TIME_RANGE} AND clusterID='${this.props.clusterID}' GROUP by nodeID, time(${OVERVIEW_INTERVAL})`,
        options: {combineSeries: true},
      },
      {
        queryDescription: "Avg Query Latency (ms)",
        host: dataNodes,
        database: '_internal',
        text: `SELECT mean(queryDurationNs)/100000000000 FROM queryExecutor WHERE time > now() - ${OVERVIEW_TIME_RANGE} AND clusterID='${this.props.clusterID}' GROUP BY time(${OVERVIEW_INTERVAL})`,
      },
      {
        queryDescription: "Active Writes",
        host: dataNodes,
        database: '_internal',
        text: `SELECT NON_NEGATIVE_DERIVATIVE(MAX(req)) FROM "write" WHERE time > now() - ${OVERVIEW_TIME_RANGE} AND clusterID='${this.props.clusterID}' GROUP BY nodeID, time(${OVERVIEW_INTERVAL})`,
        options: {combineSeries: true},
      },
    ];

    return (
      <div className="col-lg-6">
        <div className="panel panel-minimal">
          <div className="panel-heading">
            <h2 className="panel-title">Cluster Speed</h2>
          </div>
          <div className="panel-body" style={{height: '206px'}}>
            {
              queries.map(({options, host, database, text, queryDescription}, i) => {
                return (
                  <RefreshingMiniGraph clusterID={this.props.clusterID} options={options} key={i} queries={[{host, database, text}]} autoRefresh={refreshIntervalMs} queryDescription={queryDescription}>
                    <p key={i} className="cluster-stat-empty"><span className="icon cubo"></span> Waiting for stats...</p>
                  </RefreshingMiniGraph>
                );
              })
            }
          </div>
        </div>
      </div>
    );
  },
});

export default ClusterStatsPanel;
