/* eslint-disable */
import React from 'react';
import {mount} from 'enzyme';
import sinon from 'sinon';
import * as api from 'src/shared/apis';

import {ClusterAccountsPageContainer} from 'src/cluster_accounts/containers/ClusterAccountsPageContainer';

const clusterID = '1000';

const rolesResponse = {
  data: {
    roles: [
      {
        name: 'wills_role',
        permissions: {
          '': [
            'ViewAdmin',
            'CreateDatabase',
          ],
        },
        users: [
          'will',
        ],
      },
      {
        name: 'bobs_role',
        permissions: {
          db1: [
            'Monitor',
            'CopyShard',
          ],
        },
        users: [
          'bob',
        ],
      },
    ],
  },
};

const clusterAccountsResponse = {
  data: {
    users: [
      // cluster account linked to current user should be unable to be deleted
      {
        name: 'will',
        hash: 'xxxx',
        permissions: {
          '': [
            'ViewAdmin',
          ]
        }
      },
      {
        name: "bob",
        hash: "xxxx",
        permissions: {
          '': [
            'ViewAdmin',
            'ViewChronograf',
          ]
        }
      },
      // No permissions, no roles
      {
        name: "alice",
        hash: "xxxx",
      },
    ]
  }
};

const meResponse = {
  data: {
    id: 1,
    first_name: "will",
    last_name: "watkins",
    email: "will@influxdb.com",
    admin: true,
    cluster_links: [
      {
        id: 1,
        user_id: 1,
        cluster_id: "2525931939964385968",
        cluster_user: "will",
        created_at: "2016-08-22T22:56:28.347266007-07:00",
        updated_at: "2016-08-25T18:00:41.46349321-07:00"
      }
    ],
    name: "will watkins"
  },
};

function setup(customProps = {}) {
  const props = Object.assign({}, {
    params: {
      clusterID,
    },
    addFlashMessage() {},
  }, customProps);
  return mount(<ClusterAccountsPageContainer {...props} />);
}

describe('Cluster Accounts page', function() {
  beforeEach(function() {
    this.fakes = sinon.collection;
    this.fakes.stub(api, 'getClusterAccounts').returns(Promise.resolve(clusterAccountsResponse));
    this.fakes.stub(api, 'getRoles').returns(Promise.resolve(rolesResponse));
    this.fakes.stub(api, 'meShow').returns(Promise.resolve(meResponse));
  });

  afterEach(function() {
    this.fakes.restore();
  });

  it('renders a list of cluster accounts and their roles', function(done) {
    const wrapper = setup();

    then(() => {
      expect(wrapper.find('[data-test="user-row"]').length).to.equal(clusterAccountsResponse.data.users.length);
      expect(wrapper.find('table').text()).to.match(/will/);
      expect(wrapper.find('table').text()).to.match(/wills_role/);
    }, done);
  });

  describe('after creating a new cluster account', function() {
    beforeEach(function() {
      this.wrapper = setup();
      window.$ = sinon.stub().returns({ modal: () => {} }); // More bootstrap modal requirements
      this.fakes.stub(api, 'createClusterAccount').returns(Promise.resolve({}));
      this.fakes.stub(api, 'addUsersToRole').returns(Promise.resolve({}));

      then(() => {
        this.wrapper.find('[data-test="create-cluster-account"]').simulate('click');
        this.wrapper.find('[data-test="account-name"]').node.value = 'new_user';
        this.wrapper.find('[data-test="account-password"]').node.value = 'password';
        this.wrapper.find('[data-test="cluster-account-form"]').simulate('submit');
      });
    });

    it('makes the right request', function(done) {
      then(() => {
        expect(api.createClusterAccount.calledWith(clusterID, 'new_user', 'password')).to.be.true;
        expect(api.addUsersToRole.calledWith(clusterID, 'wills_role', ['new_user'])).to.be.true;
      }, done);
    });

    it('adds the cluster account to the table', function(done) {
      then(() => {
        expect(this.wrapper.find('[data-test="user-row"]').length).to.equal(clusterAccountsResponse.data.users.length + 1);
      }, done);
    });
  });

  describe('after deleting a cluster account', function() {
    it('makes the right request');
    it('removes the cluster account from the table');
  });

  describe('when the cluster account is linked to the current user', function() {
    it('disables the "Delete" control');
  });
});
/* eslint-enable */
