import React from 'react';
import {shallow} from 'enzyme';
import RetentionPoliciesList from 'src/retention_policies/components/RetentionPoliciesList';
import RetentionPolicyCard from 'src/retention_policies/components/RetentionPolicyCard';

function setup(customProps = {}) {
  const defaultProps = {
    retentionPolicies: [
      {name: "default", duration: "0", shardGroupDuration: "168h0m0s", replication: 2, isDefault: true},
      {name: "other", duration: "0", shardGroupDuration: "168h0m0s", replication: 2, isDefault: false},
    ],
    shardDiskUsage: {
      1: [
        {diskUsage: 100, nodeID: "localhost:8088"},
        {diskUsage: 100, nodeID: "localhost:8188"},
      ],
      2: [
        {diskUsage: 100, nodeID: "localhost:8088"},
        {diskUsage: 100, nodeID: "localhost:8188"},
      ]
    },
    shards: {
      'db1..default': [
        {shardId: '1', database: "stress", retentionPolicy: "default", shardGroup: '9', startTime: "2006-01-02T00:00:00Z", endTime: "2006-01-09T00:00:00Z", owners: [1, 2]},
      ],
      'db1..other': [
        {shardId: '2', database: "stress", retentionPolicy: "default", shardGroup: '9', startTime: "2006-01-02T00:00:00Z", endTime: "2006-01-09T00:00:00Z", owners: [1, 2]},
      ]
    },
    onDropShard: () => {},
    selectedDatabase: 'db1',
  };
  const props = Object.assign({}, defaultProps, customProps);
  return shallow(<RetentionPoliciesList {...props} />);
}

describe('RetentionPolicies.Components.RetentionPoliciesList', () => {
  it('renders a RetentionPolicyCard for each retention policy', () => {
    const wrapper = setup();
    const cards = wrapper.find(RetentionPolicyCard);

    expect(cards.length).to.equal(2);
  });

  describe('with no shard disk usage data', function() {
    it('doesn\'t throw', function() {
      expect(setup.bind(this, {shardDiskUsage: {}})).not.to.throw();
    });
  });
});
