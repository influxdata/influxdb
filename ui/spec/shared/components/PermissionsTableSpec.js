import PermissionsTable from 'src/shared/components/PermissionsTable';
import React from 'react';
import {shallow} from 'enzyme';
import sinon from 'sinon';

describe('Shared.Components.PermissionsTable', function() {
  it('renders a row for each permission', function() {
    const permissions = [
      {name: 'ViewChronograf', displayName: 'View Chronograf', description: 'Can use Chronograf tools', resources: ['db1']},
      {name: 'Read', displayName: 'Read', description: 'Can read data', resources: ['']},
    ];

    const wrapper = shallow(
      <PermissionsTable
        permissions={permissions}
        showAddResource={true}
        onRemovePermission={sinon.spy()}
      />
    );

    expect(wrapper.find('tr').length).to.equal(2);
    expect(wrapper.find('table').text()).to.match(/View Chronograf/);
    expect(wrapper.find('table').text()).to.match(/db1/);
    expect(wrapper.find('table').text()).to.match(/Read/);
    expect(wrapper.find('table').text()).to.match(/All Databases/);
  });

  it('only renders the control to add a resource when specified', function() {
    const wrapper = shallow(
      <PermissionsTable
        permissions={[{name: 'Read', displayName: 'Read', description: 'Can read data', resources: ['']}]}
        showAddResource={false}
        onRemovePermission={sinon.spy()}
      />
    );

    expect(wrapper.find('.pill-add').length).to.equal(0);
  });

  it('only renders the "Remove" control when a callback is provided', function() {
    const wrapper = shallow(
      <PermissionsTable
        permissions={[{name: 'Read', displayName: 'Read', description: 'Can read data', resources: ['']}]}
        showAddResource={true}
      />
    );

    expect(wrapper.find('.remove-permission').length).to.equal(0);
  });

  describe('when a user clicks "Remove"', function() {
    it('fires a callback', function() {
      const permission = {name: 'Read', displayName: 'Read', description: 'Can read data', resources: ['']};
      const cb = sinon.spy();
      const wrapper = shallow(
        <PermissionsTable
          permissions={[permission]}
          showAddResource={false}
          onRemovePermission={cb}
        />
      );

      wrapper.find('button[children="Remove"]').at(0).simulate('click');

      expect(cb.calledWith(permission)).to.be.true;
    });
  });
});
