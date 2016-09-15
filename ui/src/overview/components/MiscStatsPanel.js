import React, {PropTypes} from 'react';
import {formatBytes} from 'utils/formatting';
import {diskBytesFromShard} from 'shared/parsing/diskBytes';
import {clusterDiskUsage} from 'shared/apis/stats';
import MiniGraph from 'shared/components/MiniGraph';
import AutoRefresh from 'shared/components/AutoRefresh';
const RefreshingMiniGraph = AutoRefresh(MiniGraph);
import {OVERVIEW_TIME_RANGE, OVERVIEW_INTERVAL} from '../constants';

const MiscStatsPanel = React.createClass({
  propTypes: {
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    clusterID: PropTypes.string.isRequired,
    refreshIntervalMs: PropTypes.number.isRequired,
  },
  getInitialState() {
    return {
      diskUsed: 0,
      influxVersion: null,
    };
  },
  componentDidMount() {
    clusterDiskUsage(this.props.dataNodes, this.props.clusterID).then((res) => {
      this.setState({
        diskUsed: diskBytesFromShard(res.data).bytes,
        influxVersion: res.headers['x-influxdb-version'],
      });
    });
  },
  render() {
    const {influxVersion, diskUsed} = this.state;
    const {dataNodes, refreshIntervalMs} = this.props;
    const queries = [
      {
        queryDescription: 'Bytes Allocated',
        host: dataNodes,
        database: '_internal',
        text: `select max(Alloc) AS bytes_allocated from runtime where time > now() - ${OVERVIEW_TIME_RANGE} AND clusterID='${this.props.clusterID}' group by time(${OVERVIEW_INTERVAL})`,
      },
      {
        queryDescription: 'Heap Bytes',
        host: dataNodes,
        database: '_internal',
        text: `select max(HeapInUse) AS heap_bytes from runtime where time > now() - ${OVERVIEW_TIME_RANGE} AND clusterID='${this.props.clusterID}' group by time(${OVERVIEW_INTERVAL})`,
      },
    ];

    return (
      <div className="col-lg-6">
        <div className="panel panel-minimal">
          <div className="panel-heading">
            <h2 className="panel-title">Misc Stats</h2>
          </div>
          <div className="panel-body" style={{height: '206px'}}>
            <div className="cluster-stat-2x">
              <div className="influx-version">
                <span className="cluster-stat-2x--label">Version:</span><span className="cluster-stat-2x--number">{influxVersion}</span>
              </div>
              <div className="disk-util">
                <span className="cluster-stat-2x--label">Disk Use:</span><span className="cluster-stat-2x--number">{formatBytes(diskUsed)}</span>
              </div>
            </div>
            <div className="top-stuff">
              {
                queries.map(({host, database, text, queryDescription}, i) => {
                  return (
                    <RefreshingMiniGraph key={i} clusterID={this.props.clusterID} queries={[{host, database, text}]} autoRefresh={refreshIntervalMs} queryDescription={queryDescription}>
                      <p className="cluster-stat-empty"><span className="icon cubo"></span> Waiting for stats...</p>
                    </RefreshingMiniGraph>
                  );
                })
              }
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default MiscStatsPanel;
