import React, {PropTypes} from 'react';
import _ from 'lodash';
import PageHeader from '../components/PageHeader';
import UsersTable from 'shared/components/UsersTable';
import InviteUserModal from '../components/modals/InviteUser';
import FlashMessages from 'shared/components/FlashMessages';
import DeleteUserModal from '../components/modals/DeleteUsers';
import {
  getWebUsers,
  deleteWebUsers,
  meShow,
  createWebUser,
  batchCreateUserClusterLink,
} from 'shared/apis/index';

const {string, func} = PropTypes;
const UsersPage = React.createClass({
  propTypes: {
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
    }).isRequired,
    addFlashMessage: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      filterText: '',
      users: [],
      userToDelete: {
        id: 0,
        name: '',
      },
      me: {
        id: 0,
      },
      clusterLinks: {},
    };
  },

  componentDidMount() {
    this.refreshUsers();
    meShow().then(({data}) => {
      this.setState({me: data});
    });
  },

  handleCreateWebUser(user) {
    const {addFlashMessage} = this.props;

    createWebUser(user).then(({data}) => {
      batchCreateUserClusterLink(data.id, this.getClusterLinks()).then(() => {
        this.refreshUsers();
        addFlashMessage({
          type: 'success',
          text: 'User added successfully',
        });
      }).catch((err) => {
        this.handleFormError(err, 'Problem adding user');
      });
    }).catch((err) => {
      this.handleFormError(err, 'Problem adding cluster account');
    });
  },

  handleSelectClusterAccount({clusterID, accountName}) {
    const clusterLinks = Object.assign({}, this.state.clusterLinks, {
      [clusterID]: accountName,
    });
    this.setState({clusterLinks});
  },

  handleUserInput(filterText) {
    this.setState({filterText});
  },

  handleUserToDelete(userToDelete) {
    this.setState({userToDelete});
  },

  handleConfirmDeleteUser(userID) {
    deleteWebUsers(userID).then(() => {
      this.refreshUsers();
      this.props.addFlashMessage({
        type: 'success',
        text: 'User successfully deleted!',
      });
    }).catch(() => {
      this.props.addFlashMessage({
        type: 'error',
        text: 'There was a problem deleting the user...',
      });
    });
  },

  getClusterLinks() {
    return Object.keys(this.state.clusterLinks).map((clusterID) => {
      return {
        cluster_id: clusterID,
        cluster_user: this.state.clusterLinks[clusterID],
      };
    });
  },

  refreshUsers() {
    getWebUsers(this.props.params.clusterID).then(({data}) => {
      this.setState({users: data});
    });
  },

  handleFormError(err, text) {
    const formErrors = _.get(err, 'response.data.errors', null);
    const messages = formErrors ? Object.keys(formErrors).map(k => formErrors[k]) : text;
    console.error(messages); // eslint-disable-line no-console
    this.props.addFlashMessage({
      type: 'error',
      text: messages,
    });
  },

  render() {
    const {users, filterText, me} = this.state;
    const {params: {clusterID}} = this.props;
    const filteredUsers = users.filter((user) => user.name.toLowerCase().indexOf(filterText.toLowerCase()) > -1);

    return (
      <div id="user-index-page" className="js-user-index" data-cluster-id={clusterID}>
        <PageHeader activeCluster={clusterID} />

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">

              <div className="panel panel-minimal">
                <div className="panel-heading u-flex u-jc-space-between u-ai-center">
                  <h2 className="panel-title"></h2>
                  <SearchBar filterText={this.state.filterText} onUserInput={this.handleUserInput} />
                </div>
                <div className="panel-body">
                  <UsersTable
                    me={me}
                    users={filteredUsers}
                    activeCluster={clusterID}
                    onUserToDelete={this.handleUserToDelete}
                  />
                </div>
              </div>
            </div>
          </div>
        </div>
        <InviteUserModal clusterID={clusterID} onSelectClusterAccount={this.handleSelectClusterAccount} onCreateWebUser={this.handleCreateWebUser} />
        <DeleteUserModal handleConfirmDeleteUser={this.handleConfirmDeleteUser} userToDelete={this.state.userToDelete}/>
      </div>
    );
  },
});

const SearchBar = React.createClass({
  propTypes: {
    onUserInput: func.isRequired,
    filterText: string.isRequired,
  },

  handleChange() {
    this.props.onUserInput(this._filterText.value);
  },

  render() {
    return (
      <div className="users__search-widget input-group">
        <div className="input-group-addon">
          <span className="icon search" aria-hidden="true"></span>
        </div>
        <input
          type="text"
          className="form-control"
          placeholder="Filter Web Users"
          value={this.props.filterText}
          ref={(ref) => this._filterText = ref}
          onChange={this.handleChange}
        />
      </div>
    );
  },
});

export default FlashMessages(UsersPage);
