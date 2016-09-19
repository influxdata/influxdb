import React from 'react';
import {renderIntoDocument, Simulate} from 'react-addons-test-utils';
import {findDOMNode} from 'react-dom';
import sinon from 'sinon';
import RetentionPoliciesHeader from 'src/retention_policies/components/RetentionPoliciesHeader';

describe('RetentionPolicies.Components.RetentionPoliciesHeader', () => {
  it('renders the currently selected database', () => {
    const component = renderIntoDocument(
      <RetentionPoliciesHeader
        databases={['db1', 'db2']}
        selectedDatabase={'db1'}
        onChooseDatabase={() => {}}
      />
    );

    const node = findDOMNode(component).querySelectorAll('.dropdown-toggle')[0];

    expect(node.textContent).to.equal('db1');
  });

  it('renders a dropdown list of all databases', () => {
    const component = renderIntoDocument(
      <RetentionPoliciesHeader
        databases={['db1', 'db2']}
        selectedDatabase={'db1'}
        onChooseDatabase={() => {}}
      />
    );

    const dropdownItems = findDOMNode(component).querySelectorAll('.dropdown-menu li');

    expect(dropdownItems[0].textContent).to.equal('db1');
    expect(dropdownItems[1].textContent).to.equal('db2');
  });

  describe('when a database is selected', () => {
    it('fires a callback', () => {
      const onChooseDatabaseCallback = sinon.spy();

      const component = renderIntoDocument(
        <RetentionPoliciesHeader
          databases={['db1', 'db2']}
          selectedDatabase={'db1'}
          onChooseDatabase={onChooseDatabaseCallback}
        />
      );

      Simulate.click(findDOMNode(component).querySelectorAll('.dropdown-menu li')[0]);

      expect(onChooseDatabaseCallback).to.have.been.calledWith('db1');
    });
  });
});
