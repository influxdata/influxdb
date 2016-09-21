import React from 'react';
import TestUtils, {renderIntoDocument, scryRenderedComponentsWithType} from 'react-addons-test-utils';
import {findDOMNode} from 'react-dom';
import {mount, shallow} from 'enzyme';
import sinon from 'sinon';
import * as api from 'src/shared/apis';

import {OverviewPage} from 'src/overview/containers/OverviewPage';
import ClusterStatsPanel from 'src/overview/components/ClusterStatsPanel';
import MiscStatsPanel from 'src/overview/components/MiscStatsPanel';
import NodeTable from 'src/overview/components/NodeTable';

const clusterID = '1000';
const showClustersResponse = {
  data: [
    {
      tcpAddr: 'localhost:8088',
      httpAddr: 'localhost:8086',
      id: 1,
      status: 'joined'
    },
    {
      tcpAddr: 'localhost:8188',
      httpAddr: 'localhost:8186',
      id: 2,
      status: 'joined'
    }
  ],
  meta: [
    {
      addr: 'localhost:8091'
    }
  ]
};

function setup(customProps = {}) {
  const defaultProps = {
    dataNodes: ['localhost:8086'],
    params: {clusterID},
    addFlashMessage: function() {},
  };
  const props = Object.assign({}, defaultProps, customProps);
  return mount(<OverviewPage {...props} />);
}

xdescribe('Overview.Containers.OverviewPage', function() {
  it('renders a spinner initially', function() {
    const wrapper = shallow(<OverviewPage dataNodes={['localhost:8086']} params={{clusterID}} addFlashMessage={() => {}} />);

    expect(wrapper.contains(<div className="page-spinner" />)).to.be.true;
  });

  describe('after being mounted', function() {
    let stub;
    beforeEach(function() {
      stub = sinon.stub(api, 'showCluster').returns(Promise.resolve({
        data: showClustersResponse,
      }));
    });

    afterEach(function() {
      stub.restore();
    });

    it('fetches cluster information after being mounted', function() {
      setup();

      expect(api.showCluster.calledWith(clusterID)).to.be.true;
    });

    it('hides the spinner', function(done) {
      const wrapper = setup();

      setTimeout(() => {
        expect(wrapper.contains(<div className="page-spinner" />)).to.be.false;
        done();
      }, 0);
    });

    it('fetches cluster information after being mounted', function() {
      setup();

      expect(api.showCluster.calledWith(clusterID)).to.be.true;
    });

    it('renders the correct panels', function(done) {
      const wrapper = setup();

      setTimeout(() => {
        expect(wrapper.find(ClusterStatsPanel)).to.have.length(1);
        expect(wrapper.find(MiscStatsPanel)).to.have.length(1);
        expect(wrapper.find(NodeTable)).to.have.length(1);
        done();
      }, 0);
    });

    it('passes the correct props to NodeTable', function(done) {
      const wrapper = setup();

      setTimeout(() => {
        const table = wrapper.find(NodeTable);
        expect(table.props().cluster).to.eql(showClustersResponse);
        expect(table.props().dataNodes).to.eql(['localhost:8086']);
        expect(table.props().clusterID).to.equal(clusterID);
        done();
      }, 0);
    });
  });
});
