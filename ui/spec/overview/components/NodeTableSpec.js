import NodeTable from 'src/overview/components/NodeTable';
import NodeTableRow from 'src/overview/components/NodeTableRow';
import React from 'react';
import TestUtils, {renderIntoDocument, scryRenderedComponentsWithType} from 'react-addons-test-utils';
import {findDOMNode} from 'react-dom';

describe('Overview.Components.NodeTable', () => {
  it('renders a NodeTableRow for each node (meta + data)', () => {
    const cluster = {
      "data": [
        {
          "tcpAddr": "localhost:8088",
          "httpAddr": "localhost:8086",
          "id": 2,
          "status": "joined"
        },
        {
          "tcpAddr": "localhost:8188",
          "httpAddr": "localhost:8186",
          "id": 6,
          "status": "joined"
        }
      ],
      "meta": [
        {
          "addr": "localhost:8091"
        }
      ],
      "id": "a cluster id"
    };

    const component = renderIntoDocument(
      <NodeTable
        clusterID='a cluster id'
        cluster={cluster}
        dataNodes={['localhost:8088', 'localhost:8188']}
        refreshIntervalMs={2000}
      />
    );
    const nodeRows = scryRenderedComponentsWithType(component, NodeTableRow);

    expect(nodeRows.length).to.equal(3);
  });
});
