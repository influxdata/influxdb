/* eslint-disable */
import React from 'react';
import {mount} from 'enzyme';
import sinon from 'sinon';
import * as api from 'src/shared/apis';

import {ClusterAccountContainer} from 'src/cluster_accounts/containers/ClusterAccountContainer';
import RolePanels from 'src/shared/components/RolePanels';
// import {Tab, TabList} from 'src/shared/components/Tabs';

const clusterID = '1000';
const accountID = 'nedstark';

const rolesResponse = {
  data: {
    roles: [
      {
        name: "Role1",
        permissions: {
          "": [
            "ViewAdmin",
            "CreateDatabase",
          ],
        },
        users: [
          "will",
        ],
      },
      {
        name: "Role2",
        permissions: {
          db1: [
            "Monitor",
            "CopyShard",
          ],
        },
        users: [
          "will",
        ],
      },
      {
        name: "Role3",
      },
    ],
  },
};

const clusterAccountResponse = {
  data: {
    users: [
      {
        name: "will",
        hash: "xxxx",
        permissions: {
          "": [
            "ViewAdmin",
          ]
        }
      }
    ]
  }
}

function setup(customProps = {}) {
  const props = Object.assign({}, {
    params: {
      clusterID,
      accountID,
    },
    router: {
      push: sinon.spy(),
    },
    addFlashMessage() {},
    dataNodes: ['localhost:8086'],
  }, customProps);
  return mount(<ClusterAccountContainer {...props} />);
}

describe('Cluster Account page', function() {
  beforeEach(function() {
    this.fakes = sinon.collection;
    this.fakes.stub(api, 'getClusterAccount').returns(Promise.resolve(clusterAccountResponse));
    this.fakes.stub(api, 'getRoles').returns(Promise.resolve(rolesResponse));
  });

  afterEach(function() {
    this.fakes.restore();
  });

  describe('Roles tab', function() {
    it('renders a list of roles', function(done) {
      const wrapper = setup();

      then(() => {
        expect(wrapper.find(RolePanels).length).to.equal(1);
      }, done);
    });

    describe('after confirming to remove a role from the cluster account', function() {
      beforeEach(function() {
        window.$ = sinon.stub().returns({ modal: () => {} }); // More bootstrap modal requirements
        this.fakes.stub(api, 'removeAccountsFromRole').returns(Promise.resolve({data: {}}));
        this.wrapper = setup();
        then(() => {
          this.wrapper.find(RolePanels).find('button[children="Remove"]').at(0).simulate('click');
          this.wrapper.find('#removeAccountFromRoleModal button[children="Remove"]').simulate('click');
        });
      });

      it('makes the right request', function(done) {
        then(() => {
          expect(api.removeAccountsFromRole.called).to.be.true;
          expect(api.removeAccountsFromRole.calledWith(clusterID, 'Role1', [accountID])).to.be.true;
        }, done);
      });

      it('removes the role from the page', function(done) {
        then(() => {
          expect(this.wrapper.find(RolePanels).text()).not.to.match(/Role1/);
        }, done);
      });
    });

    describe('after choosing a role to add to a user', function() {
      beforeEach(function() {
        window.$ = sinon.stub().returns({ modal: () => {} }); // More bootstrap modal requirements
        this.fakes.stub(api, 'addAccountsToRole').returns(Promise.resolve({data: {}}));
        this.wrapper = setup();
        then(() => {
          this.wrapper.find('button[children="Add to Role"]').simulate('click');
          this.wrapper.find('#addRoleModal select').simulate('change', {target: {value: 'Role3'}});
          this.wrapper.find('#addRoleModal form').simulate('submit');
        });
      });

      it('makes the right request', function(done) {
        then(() => {
          expect(api.addAccountsToRole.calledWith(clusterID, 'Role3', [accountID])).to.be.true;
        }, done);
      });

      it('adds the role to the page', function(done) {
        then(() => {
          expect(this.wrapper.find(RolePanels).text()).to.match(/Role3/);
        }, done);
      });
    });
  });
});
/* eslint-enable */
