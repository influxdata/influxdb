/* eslint-disable */
import React from 'react';
import {mount} from 'enzyme';
import sinon from 'sinon';
import * as api from 'src/shared/apis';

import RolesPageContainer from 'src/access_control/containers/RolesPageContainer';

const rolesResponse = {
  data: {
    roles: [
      {
        name: "Marketing",
        permissions: {
          "": [
            "ViewAdmin",
            "CreateDatabase",
          ],
        },
        users: [
          "will@influxdb.com",
          "jon@influxdb.com",
        ],
      },
      {
        name: "Global Admin",
        permissions: {
          db1: [
            "Monitor",
            "CopyShard",
          ],
        },
        users: [
          "will@influxdb.com",
        ],
      },
      {
        name: "Role with no permissions"
      },
    ],
  },
};

const clusterID = '1000';

function setup(customProps = {}) {
  const props = Object.assign({}, {
    params: {
      clusterID,
    },
    addFlashMessage: () => {},
  }, customProps);
  return mount(<RolesPageContainer {...props} />);
}

function submitRoleForm(wrapper, roleName = 'new role') {
  const input = wrapper.find('#roleName');
  const form = wrapper.find('#createRoleModal form');

  input.node.value = roleName;
  form.simulate('submit');
}

describe('Roles page', function() {
  // TODO: tests seem better when using the fake server, but for some reason it isn't cooperating.
  // In the the meantime I'm just stubbing AJAX related.
  // before(function() { this.server = sinon.fakeServer.create(); });
  // after(function()  { this.server.restore() });
  // beforeEach(function() {
  //   this.server.respondWith("GET", '/api/int/v1/clusters/1000/roles', [
  //     200, { "Content-Type": "application/json" },
  //     JSON.stringify(rolesResponse)
  //   ]);
  //   this.server.respondWith("POST", '/api/int/v1/clusters/1000/roles', [
  //     201, { "Content-Type": "application/json" },
  //     JSON.stringify(createRoleResponse)
  //   ]);
  // });

  beforeEach(function() {
    this.fakes = sinon.collection;
    this.fakes.stub(api, 'getRoles').returns(Promise.resolve(rolesResponse));
  });

  afterEach(function() {
    this.fakes.restore();
  });

  it('renders a panel for each role', function(done) {
    const wrapper = setup();
    then(() => {
      expect(wrapper.find('.roles .panel').length).to.equal(3);
      expect(wrapper.find('.roles .panel-heading').at(0).text()).to.match(/Marketing/);
      expect(wrapper.find('.roles .panel-heading').at(1).text()).to.match(/Global Admin/);
    }, done);
  });

  it('renders how many users belong to the role', function(done) {
    const wrapper = setup();
    then(() => {
      expect(wrapper.find('.roles .panel-heading').at(0).text()).to.match(/2 Users/);
      expect(wrapper.find('.roles .panel-heading').at(1).text()).to.match(/1 User/);
    }, done);
  });

  it('renders a "Go to role" link', function(done) {
    const wrapper = setup();
    then(() => {
      const links = wrapper.find('a').findWhere(n => n.text().match(/Go To Role/));
      expect(links.length).to.equal(3);
    }, done);
  });

  describe('when a role has no permissions', function() {
    it('renders an empty state', function(done) {
      const wrapper = setup();
      then(() => {
        expect(wrapper.find('.roles .generic-empty-state').length).to.equal(1);
      }, done);
    });
  });

  it('renders a list of permissions and locations for each role', function(done) {
    const wrapper = setup();
    then(() => {
      expect(wrapper.find('tr').at(0).text()).to.match(/View Admin/);
      expect(wrapper.find('tr').at(1).text()).to.match(/Create Databases/);
    }, done);
  });

  it('renders all databases associated with each permission', function(done) {
    const wrapper = setup();
    then(() => {
      expect(wrapper.find('tr').at(0).text()).to.match(/All Databases/);
      expect(wrapper.find('table').at(1).find('tr').at(0).text()).to.match(/db1/);
    }, done);
  });

  it('renders a "create role" button', function(done) {
    const wrapper = setup();
    then(() => {
      expect(wrapper.find('button[children="Create Role"]').length).to.equal(1);
    }, done);
  });

  describe('after clicking "Create role"', function() {
    beforeEach(function() {
      window.$ = sinon.stub().returns({modal: () => {}}); // More bootstrap modal requirements
    });

    it('makes the right request', function(done) {
      this.fakes.stub(api, 'createRole').returns(Promise.resolve({data: {}}));
      const wrapper = setup();
      submitRoleForm(wrapper, 'new role');

      then(() => {
        expect(api.createRole.calledWith('1000', 'new role')).to.be.true;
      }, done);
    });

    // Un-xit me when https://github.com/influxdata/plutonium/issues/538 is resolved
    xit('adds the role to the page', function(done) {
      this.fakes.stub(api, 'createRole').returns(Promise.resolve({data: {}}));
      const startingRolesLen = rolesResponse.data.roles.length;
      const wrapper = setup();

      submitRoleForm(wrapper, 'new role');

      then(() => {
        expect(wrapper.find('.roles .panel').length).to.equal(startingRolesLen + 1);
        expect(wrapper.find('.roles .panel').at(startingRolesLen).text()).to.match(/new role/);
      }, done);
    });
  });
});
/* eslint-enable */
