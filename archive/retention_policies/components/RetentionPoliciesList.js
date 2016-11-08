import React, {PropTypes} from 'react';

import RetentionPolicyCard from './RetentionPolicyCard';

const {string, arrayOf, shape, func} = PropTypes;
export default React.createClass({
  propTypes: {
    retentionPolicies: arrayOf(shape()).isRequired,
    shardDiskUsage: shape(),
    shards: shape().isRequired,
    selectedDatabase: string.isRequired,
    onDropShard: func.isRequired,
  },

  render() {
    const {shardDiskUsage, retentionPolicies, onDropShard, shards, selectedDatabase} = this.props;

    return (
      <div className="row">
        <div className="col-md-12">
          <h3 className="deluxe fake-panel-title">Retention Policies</h3>
          <div className="panel-group retention-policies" id="accordion" role="tablist" aria-multiselectable="true">
            {retentionPolicies.map((rp, i) => {
              const ss = shards[`${selectedDatabase}..${rp.name}`] || [];
              /**
               * We use the `/show-shards` endpoint as 'source of truth' for active shards in the cluster.
               * Disk usage has to be fetched directly from InfluxDB, which means we'll have stale shard
               * data (the results will often include disk usage for shards that have been removed). This
               * ensures we only use active shards when we calculate disk usage.
               */
              const newDiskUsage = {};
              ss.forEach((shard) => {
                (shardDiskUsage[shard.shardId] || []).forEach((d) => {
                  if (!shard.owners.map((o) => o.tcpAddr).includes(d.nodeID)) {
                    return;
                  }

                  if (newDiskUsage[shard.shardId]) {
                    newDiskUsage[shard.shardId].push(d);
                  } else {
                    newDiskUsage[shard.shardId] = [d];
                  }
                });
              });

              return (
                <RetentionPolicyCard
                  key={rp.name}
                  onDelete={() => {}}
                  rp={rp}
                  shards={ss}
                  index={i}
                  shardDiskUsage={newDiskUsage}
                  onDropShard={onDropShard}
                />
              );
            })}
          </div>
        </div>
      </div>
    );
  },
});
