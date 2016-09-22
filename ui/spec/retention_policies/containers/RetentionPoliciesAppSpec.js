import React from 'react';
import TestUtils, {renderIntoDocument} from 'react-addons-test-utils';
import {findDOMNode} from 'react-dom';
import sinon from 'sinon';

import RetentionPoliciesApp from 'src/retention_policies/containers/RetentionPoliciesApp';

xdescribe('RetentionPolicies.Containers.RetentionPoliciesApp', () => {
  let server;
  before(() => { server = sinon.fakeServer.create(); });
  after(() =>  { server.restore() });

  let component;
  beforeEach(() => {
    server.respondWith("GET", "/proxy?proxy_url=http%3A%2F%2Flocalhost%3A8086%2Fquery%3Fepoch%3Dms%26q%3DSHOW%20DATABASES",
      [200, { "Content-Type": "application/json" },
      '{"results":[{"series":[{"name":"databases","columns":["name"],"values":[["_internal"],["hello"],["demo"],["asdf"]]}]}]}']);

    server.respondWith("GET", "/proxy?proxy_url=http%3A%2F%2Flocalhost%3A8086%2Fquery%3Fepoch%3Dms%26q%3DSHOW%20RETENTION%20POLICIES%20ON%20%22_internal%22",
      [200, { "Content-Type": "application/json" },
      '{"results":[{"series":[{"columns":["name","duration","shardGroupDuration","replicaN","default"],"values":[["monitor","168h0m0s","24h0m0s",2,true],["asdf","24h0m0s","1h0m0s",1,false]]}]}]}']);

    server.respondWith("GET", "/proxy?proxy_url=http%3A%2F%2Flocalhost%3A8086%2Fquery%3Fepoch%3Dms%26q%3DSELECT%20last(diskBytes)%20FROM%20%22shard%22%20WHERE%20%22database%22%3D%27_internal%27%20AND%20clusterID%100%27%20GROUP%20BY%20nodeID%2C%20path%2C%20retentionPolicy%26db%3D_internal",
      [200, { "Content-Type": "application/json" },
      '{"results":[{"series":[{"name":"shard","tags":{"nodeID":"localhost:8088","path":"/Users/will/.influxdb/data/_internal/monitor/15","retentionPolicy":"monitor"},"columns":["time","last"],"values":[[1469068381000000000,1036740]]}]},{"series":[{"name":"shard","tags":{"nodeID":"localhost:8088","path":"/Users/will/.influxdb/data/_internal/monitor/16","retentionPolicy":"monitor"},"columns":["time","last"],"values":[[1469068381000000000,8771759]]}]}]}']);

      server.respondWith("GET", "/api/int/v1/clusters/show-shards",
      [200, { "Content-Type": "application/json" },
      '[{"id": "1", "database": "_internal", "retention-policy": "monitor", "replica-n": 2, "shard-group-id": "1", "start-time": "2016-07-15T00:00:00Z", "end-time": "2016-07-16T00:00:00Z", "expire-time": "2016-07-23T00:00:00Z", "truncated-at": "0001-01-01T00:00:00Z", "owners": [ { "id": "2", "tcpAddr": "localhost:8088" } ] }, { "id": "2", "database": "_internal", "retention-policy": "monitor", "replica-n": 2, "shard-group-id": "2", "start-time": "2016-07-16T00:00:00Z", "end-time": "2016-07-17T00:00:00Z", "expire-time": "2016-07-24T00:00:00Z", "truncated-at": "0001-01-01T00:00:00Z", "owners": [] } ]']);

    component = renderIntoDocument(
      <RetentionPoliciesApp dataNodes={['localhost:8086']} params={{clusterID: '100'}} addFlashMessage={() => {}} />
    );
  });

  it('fetches all databases in a cluster', (done) => {
    server.respond();
    setTimeout(() => {
      expect(server.requests[0].url).to.match(/SHOW%20DATABASES/);
      done();
    }, 0);
  });
});
