import React from 'react';
import TestUtils, {renderIntoDocument, scryRenderedComponentsWithType} from 'react-addons-test-utils';
import sinon from 'sinon';
import {mount} from 'enzyme';
import * as metaQueryApi from 'src/shared/apis/metaQuery';

import {QueriesPage} from 'src/queries/containers/QueriesPage';

const clusterID = '1000';

const showDatabasesResponse = {
  data: {
    results: [
      {
        series: [
          {
            name: 'databases',
            columns: [
              'name',
            ],
            values: [
              ['db1'],
              ['db2'],
            ]
          }
        ]
      }
    ]
  }
};

const showQueriesResponse = {
  data: {
    results: [
      {
        series: [
          {
            columns: [
              'qid',
              'node_id',
              'tcp_host',
              'query',
              'database',
              'duration'
            ],
            values: [
              [
                100,
                2,
                'localhost:8088',
                'SHOW QUERIES',
                'db1',
                '1s'
              ],
              [
                200,
                2,
                'localhost:8088',
                'SELECT foo FROM bar',
                'db2',
                '10s'
              ]
            ]
          }
        ]
      }
    ]
  }
};

function setup(customProps = {}) {
  const defaultProps = {
    dataNodes: ['localhost:8086'],
    params: {
      clusterID,
    },
  };
  const props = Object.assign({}, defaultProps, customProps);
  return mount(<QueriesPage {...props} />);
}

describe('Queries.Containers.QueriesPage', function() {
  describe('after being mounted', function() {
    let fakes;
    beforeEach(function() {
      fakes = sinon.collection;
      fakes.stub(metaQueryApi, 'showDatabases').returns(Promise.resolve(showDatabasesResponse));
      fakes.stub(metaQueryApi, 'showQueries').returns(Promise.resolve(showQueriesResponse));
    });

    afterEach(function() {
      fakes.restore();
    });

    it('fetches a list of databases from InfluxDB after being mounted', function(done) {
      const wrapper = setup();

      setTimeout(() => {
        expect(metaQueryApi.showDatabases.called).to.be.true;
        done();
      }, 0);
    });

    it('fetches all running queries for each database', function(done) {
      const wrapper = setup();

      setTimeout(() => {
        expect(metaQueryApi.showQueries.calledTwice).to.be.true;
        expect(metaQueryApi.showQueries.calledWith(['localhost:8086'], 'db1')).to.be.true;
        expect(metaQueryApi.showQueries.calledWith(['localhost:8086'], 'db2')).to.be.true;
        done();
      }, 0);
    });

    describe('after clicking the \'kill\' button', function() {
      it('tracks which query is trying to be destroyed', function(done) {
        const wrapper = setup();

        setTimeout(() => {
          wrapper.find('tr button').at(0).simulate('click')

          wrapper
            .find('tr')
            .findWhere(n => n.text().match(/SHOW QUERIES/))
            .find('button').simulate('click');

          // Not great to make assertions directly on state, but because we don't have full
          // control of modals in React I think this is worth testing here.
          expect(wrapper.state().queryIDToKill).to.equal('100');
          done();
        }, 0);
      });
    });

    describe('after confirming to kill a query', function() {
      it('fires a callback with the correct query ID', function(done) {
        const wrapper = setup();

        setTimeout(() => {
          const stub = sinon.stub(metaQueryApi, 'killQuery').returns(Promise.resolve());
          wrapper
            .find('tr')
            .findWhere(n => n.text().match(/SHOW QUERIES/))
            .find('button').simulate('click');

          wrapper.find('.modal .btn-danger').simulate('click')

          expect(metaQueryApi.killQuery.calledWith(['localhost:8086'], '100')).to.be.true;

          stub.restore();
          done();
        }, 0);
      });
    });
  });

  it('fires a callback to add a flash message if InfluxDB returns an error', function(done) {
    const stub = sinon.stub(metaQueryApi, 'showDatabases').returns(Promise.resolve({
      data: {"results":[{"error":"cannot find shard id 7"}]}
    }));
    const cb = sinon.spy();

    const wrapper = setup({
      addFlashMessage: cb,
    });

    stub.restore();
    setTimeout(() => {
      expect(cb.calledWith({type: 'error', text: 'cannot find shard id 7'})).to.be.true;
      done();
    }, 0);
  });
});
