import React from 'react';
import {mount} from 'enzyme';
import sinon from 'sinon';

import RetentionPolicyCard from 'src/retention_policies/components/RetentionPolicyCard';

const SHARD = {
  shardId: '1234',
  database: "db1",
  retentionPolicy: "rp1",
  shardGroup: '9',
  startTime: "2006-01-02T00:00:00Z",
  endTime: "2006-01-09T00:00:00Z",
  owners: [
    {id: "1", tcpAddr: "localhost:8088"},
    {id: "2", tcpAddr: "localhost:8188"},
  ],
};

function setup(customProps = {}) {
  const defaultProps = {
    rp: {
      name: "rp1",
      duration: "0",
      shardGroupDuration: "168h0m0s",
      replication: 2,
      isDefault: true
    },
    shards: [SHARD],
    shardDiskUsage: {
      1234: [
        {diskUsage: 100, nodeID: "localhost:8088"},
        {diskUsage: 200, nodeID: "localhost:8188"},
      ],
    },
    index: 0,
    onDropShard: () => {},
  };

  const props = Object.assign({}, defaultProps, customProps);

  return mount(<RetentionPolicyCard {...props} />);
}

describe('RetentionPolicies.Components.RetentionPolicyCard', function() {
  before(function() {
    // Allows us to test with things like `$(selector).modal('hide') in the code
    window.$ = sinon.stub().returns({
      on: () => {},
      modal: () => {},
    });
  });

  it('renders the retention policy name, total disk usage, and if it\'s the default', function() {
    const wrapper = setup();

    const header = wrapper.find('.js-rp-card-header');

    expect(header.text()).to.match(/rp1/);
    expect(header.text()).to.match(/300 Bytes/);
    expect(header.text()).to.match(/Default/);
  });

  it('renders a row for each shard', function() {
    const wrapper = setup();

    const shards = wrapper.find('tbody tr');

    expect(shards.length).to.equal(1);
  });

  it('renders the shard ID', function() {
    const wrapper = setup();

    const shards = wrapper.find('tbody tr');

    expect(shards.text()).to.match(/1234/);
  });

  it('renders the addresses of all nodes that the shard belongs to', function() {
    const wrapper = setup();

    const shards = wrapper.find('tbody tr');

    expect(shards.text()).to.match(/localhost:8088/);
    expect(shards.text()).to.match(/localhost:8188/);
  })

  it('renders correctly formatted timestamps for a shard\'s start and end date', function() {
    const wrapper = setup();

    const shards = wrapper.find('tbody tr');

    expect(shards.text()).to.match(/2006-01-01:16 — 2006-01-08:161/);
  });

  it('renders disk usage for each node that a shard belongs to', function() {
    const wrapper = setup();

    const shards = wrapper.find('tbody tr');

    expect(shards.text()).to.match(/100 Bytes/);
    expect(shards.text()).to.match(/200 Bytes/);
  });

  describe('clicking `Drop Shard`', function() {
    it('fires a callback', function() {
      const onDropShardSpy = sinon.spy();
      const wrapper = setup({onDropShard: onDropShardSpy});

      // Because we're still using bootstrap to open modals, we can't test dropping shards
      // only using `Simulate` :(. I don't advocate this approach, we should try
      // to exercise components only via their 'public interface' -- aka the DOM.
      wrapper.setState({shardIdToDelete: SHARD.shardId});
      wrapper.find('input#confirmation').simulate('change', {target: {value: 'delete'}});
      wrapper.find('form').simulate('submit');

      expect(onDropShardSpy).to.have.been.calledWith(SHARD);
    });

    it('fails to submit unless "delete" is entered into the input', function() {
      const onDropShardSpy = sinon.spy();
      const wrapper = setup({onDropShard: onDropShardSpy});

      wrapper.setState({shardIdToDelete: SHARD.shardId});
      wrapper.find('form').simulate('submit');

      expect(onDropShardSpy).not.to.have.been.called;
    });
  });
});
