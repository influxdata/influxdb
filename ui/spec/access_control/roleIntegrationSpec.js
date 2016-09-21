/* eslint-disable */
import React from 'react';
import {mount} from 'enzyme';
import sinon from 'sinon';
import * as api from 'src/shared/apis';
import * as metaQueryApi from 'src/shared/apis/metaQuery';

import {RolePageContainer} from 'src/access_control/containers/RolePageContainer';
import {Tab, TabList} from 'src/shared/components/Tabs';

const clusterID = '1000';

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
        name: "Role with no permissions",
      },
    ],
  },
};

const roleSlug = rolesResponse.data.roles[0].name;

const databasesResponse = {
  data: {"results":[{"series":[{"name":"databases","columns":["name"],"values":[["db1", "db2"]]}]}]},
};

function setup(customProps = {}) {
  const props = Object.assign({}, {
    params: {
      clusterID,
      roleSlug,
    },
    router: {
      push: sinon.spy(),
    },
    addFlashMessage() {},
    dataNodes: ['localhost:8086'],
  }, customProps);
  return mount(<RolePageContainer {...props} />);
}

xdescribe('Role edit page', function() {
  beforeEach(function() {
    this.fakes = sinon.collection;
    this.fakes.stub(api, 'getRoles').returns(Promise.resolve(rolesResponse));
    this.fakes.stub(metaQueryApi, 'showDatabases').returns(Promise.resolve(databasesResponse));
  });

  afterEach(function() {
    this.fakes.restore();
  });

  describe('on the permissions tab', function() {
    it('renders tabs for the role\'s permissions and cluster accounts', function(done) {
      const wrapper = setup();
      then(() => {
        expect(wrapper.find(Tab).length).to.equal(2);
        expect(wrapper.find(TabList).text()).to.match(/Permissions/);
        expect(wrapper.find(TabList).text()).to.match(/Cluster Accounts/);
      }, done);
    });

    it('renders a list of permissions and locations', function(done) {
      const wrapper = setup();
      then(() => {
        expect(wrapper.find('.permissions-table tr').at(0).text()).to.match(/View Admin/);
        expect(wrapper.find('.permissions-table tr').at(1).text()).to.match(/Create Database/);
      }, done);
    });

    it('renders a "+" button to add a location to a permission', function(done) {
      const wrapper = setup();
      then(() => {
        expect(wrapper.find('.icon.plus').length).to.equal(2);
      }, done);
    });
  });

  describe('on the cluster accounts tab', function() {
    beforeEach(function() {
      this.wrapper = setup();
      then(() => {
        this.wrapper.find(Tab).at(1).simulate('click'); // Cluster accounts tab
      });
    });

    it('renders an empty state for users and roles');

    it('renders a table of cluster accounts', function(done) {
      then(() => {
        const table = this.wrapper.find('.cluster-accounts');
        expect(table.length).to.equal(1);
        expect(table.find('tr').length).to.equal(3); // Two accounts + header
      }, done);
    });
  });

  describe('after clicking "Delete role" and clicking "Confirm"', function(done) {
    beforeEach(function() {
      window.$ = sinon.stub().returns({ modal: () => {} }); // More bootstrap modal requirements
      this.fakes.stub(api, 'deleteRole').returns(Promise.resolve({
        data: {},
      }));
      this.wrapper = setup();
      then(() => {
        this.wrapper.find('button[children="Delete Role"]').simulate('click');
        this.wrapper.find('#deleteRoleModal button[children="Delete"]').simulate('click');
      });
    });

    it('makes the right request', function(done) {
      then(() => {
        expect(api.deleteRole.calledWith(clusterID, rolesResponse.data.roles[0].name)).to.be.true
      }, done);
    });

    it('navigates back to the roles index', function(done) {
      then(() => {
        expect(this.wrapper.props().router.push.calledWith('/clusters/1000/roles')).to.be.true
      }, done);
    });
    it('renders a success notification');
  });

  describe('after opening the "Add permission" modal and selecting a permission to add', function() {
    it('renders a list of available databases for that permission');

    describe('and picking a database and confirming', function() {
      it('makes the right request');
      it('updates the role\'s list of permissions');
      it('renders a success notification');
    });
  });

  describe('after opening the "Add cluster account" modal and choosing a cluster account', function() {
    it('makes the right request');
    it('renders a success notification');
  });

  describe('after removing a permission from a role', function() {
    beforeEach(function() {
      window.$ = sinon.stub().returns({ modal: () => {} }); // More bootstrap modal requirements
      this.fakes.stub(api, 'removePermissionFromRole').returns(Promise.resolve({
        data: {},
      }));
      this.wrapper = setup();
      then(() => {
        this.wrapper.find('button[children="Remove"]').at(0).simulate('click');
      });
    });

    it('makes the right request', function(done) {
      then(() => {
        const args = api.removePermissionFromRole.args[0];
        expect(args[0]).to.equal(clusterID);
        expect(args[1]).to.equal(roleSlug);
        expect(args[2].name).to.equal('ViewAdmin');
      }, done);
    });

    it('removes the permission from the page', function(done) {
      then(() => {
        expect(this.wrapper.find('.permissions-table tr').length).to.equal(1);
      }, done);
    });
  });
});
/* eslint-enable */
