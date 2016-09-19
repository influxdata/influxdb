import React, {PropTypes} from 'react';
import classNames from 'classnames';
import moment from 'moment';

import DropShardModal from './DropShardModal';

import {formatBytes, formatRPDuration} from 'utils/formatting';

/* eslint-disable no-magic-numbers */

const {func, string, shape, number, bool, arrayOf, objectOf} = PropTypes;
export default React.createClass({
  propTypes: {
    onDropShard: func.isRequired,
    rp: shape({
      name: string.isRequired,
      duration: string.isRequired,
      isDefault: bool.isRequired,
      replication: number,
      shardGroupDuration: string,
    }).isRequired,
    shards: arrayOf(shape({
      database: string.isRequired,
      startTime: string.isRequired,
      endTime: string.isRequired,
      retentionPolicy: string.isRequired,
      shardId: string.isRequired,
      shardGroup: string.isRequired,
    })),
    shardDiskUsage: objectOf(
      arrayOf(
        shape({
          diskUsage: number.isRequired,
          nodeID: string.isRequired,
        }),
      ),
    ),
    index: number, // Required to make bootstrap JS work.
  },

  formatTimestamp(timestamp) {
    return moment(timestamp).format('YYYY-MM-DD:H');
  },

  render() {
    const {index, rp, shards, shardDiskUsage} = this.props;

    const diskUsage = shards.reduce((sum, shard) => {
      // Check if we don't have any disk usage for a shard. This happens most often
      // with a new cluster before any disk usage has a chance to be recorded.
      if (!shardDiskUsage[shard.shardId]) {
        return sum;
      }

      return sum + shardDiskUsage[shard.shardId].reduce((shardSum, shardInfo) => {
        return shardSum + shardInfo.diskUsage;
      }, 0);
    }, 0);

    return (
      <div className="panel panel-default">
        <div className="panel-heading" role="tab" id={`heading${index}`}>
          <h4 className="panel-title js-rp-card-header u-flex u-ai-center u-jc-space-between">
            <a className={index === 0 ? "" : "collapsed"} role="button" data-toggle="collapse" data-parent="#accordion" href={`#collapse${index}`} aria-expanded="true" aria-controls={`collapse${index}`}>
              <span className="caret" /> {rp.name}
            </a>
            <span>
              <p className="rp-duration">{formatRPDuration(rp.duration)} {rp.isDefault ? '(Default)' : null}</p>
              <p className="rp-disk-usage">{formatBytes(diskUsage)}</p>
            </span>
          </h4>
        </div>
        <div id={`collapse${index}`} className={classNames("panel-collapse collapse", {'in': index === 0})} role="tabpanel" aria-labelledby={`heading${index}`}>
          <div className="panel-body">
            {this.renderShardTable()}
          </div>
        </div>
        <DropShardModal onConfirm={this.handleDropShard} />
      </div>
    );
  },

  renderShardTable() {
    const {shards, shardDiskUsage} = this.props;

    if (!shards.length) {
      return <div>No shards.</div>;
    }

    return (
      <table className="table shard-table">
        <thead>
          <tr>
            <th>Shard ID</th>
            <th>Time Range</th>
            <th>Disk Usage</th>
            <th>Nodes</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {shards.map((shard, index) => {
            const diskUsages = shardDiskUsage[shard.shardId] || [];
            return (
              <tr key={index}>
                <td>{shard.shardId}</td>
                <td>{this.formatTimestamp(shard.startTime)} — {this.formatTimestamp(shard.endTime)}</td>
                <td>
                  {diskUsages.length ? diskUsages.map((s) => {
                    const diskUsageForShard = formatBytes(s.diskUsage) || 'n/a';
                    return <p key={s.nodeID}>{diskUsageForShard}</p>;
                  })
                  : 'n/a'}
                </td>
                <td>
                  {diskUsages.length ? diskUsages.map((s) => <p key={s.nodeID}>{s.nodeID}</p>) : 'n/a'}
                </td>
                <td className="text-right">
                  <button data-toggle="modal" data-target="#dropShardModal" onClick={() => this.openConfirmationModal(shard)} className="btn btn-danger btn-sm" title="Drop Shard"><span className="icon trash js-drop-shard" /></button>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    );
  },

  openConfirmationModal(shard) {
    this.setState({shardIdToDelete: shard.shardId});
  },

  handleDropShard() {
    const shard = this.props.shards.filter((s) => s.shardId === this.state.shardIdToDelete)[0];
    this.props.onDropShard(shard);
    this.setState({shardIdToDelete: null});
  },
});

/* eslint-enable no-magic-numbers */
