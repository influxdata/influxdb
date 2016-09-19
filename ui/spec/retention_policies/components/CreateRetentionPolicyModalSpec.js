import React from 'react';
import {shallow, mount} from 'enzyme';
import sinon from 'sinon';

import CreateRetentionPolicyModal from 'src/retention_policies/components/CreateRetentionPolicyModal';

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
  const props = Object.assign({}, {
    onCreate: () => {},
    dataNodes: ['localhost:8086', 'localhost:8186'],
  }, customProps);
  return shallow(<CreateRetentionPolicyModal {...props} />);
}

describe('RetentionPolicies.Components.CreateRetentionPolicyModal', function() {
  before(function() {
    // Allows us to test with things like `$(selector).modal('hide') in the code
    window.$ = sinon.stub().returns({
      modal: () => {},
    });
  });

  it('renders a name input and label', function() {
    const wrapper = setup();
    expect(wrapper.find('input[type="text"]').length).to.equal(1);
  });

  it('renders a dropdown for retention policy duration', function() {
    const wrapper = setup();
    expect(wrapper.find('select#replicationFactor').length).to.equal(1);
  });

  it('renders a dropdown for replication factor', function() {
    const wrapper = setup();
    expect(wrapper.find('select#durationSelect').length).to.equal(1);
  });

  describe('after filling out and submitting the form', function() {
    it('fires a callback with the correct arguments', function() {
      const wrapper = mount(
        <CreateRetentionPolicyModal
          onCreate={sinon.spy()}
          dataNodes={['localhost:8086', 'localhost:8188']}
        />
      );

      wrapper.find('input[type="text"]').node.value = 'my new rp';
      wrapper.find('select#durationSelect').node.value = '7d';
      wrapper.find('select#replicationFactor').node.value = '2';
      wrapper.find('form').simulate('submit', { preventDefault() {} });

      expect(wrapper.props().onCreate).to.have.been.calledWith({
        rpName: 'my new rp',
        duration: '7d',
        replicationFactor: '2',
      });
    });
  });
});
