import React, {PropTypes} from 'react';
import FlashMessages from 'shared/components/FlashMessages';
import DissociateModal from '../components/modals/DissociateUserClusterLink';
import AddClusterLinksModal from '../components/modals/AddClusterLinksModal';
import {
  showUser,
  updateUser,
  deleteUserClusterLink,
  batchCreateUserClusterLink,
} from 'shared/apis';

const {func, string, shape} = PropTypes;
const WebUserEdit = React.createClass({
  propTypes: {
    addFlashMessage: func.isRequired,
    params: shape({
      userID: string,
      clusterID: string,
    }),
  },

  getInitialState() {
    return {
      linkToDissociate: {},
    };
  },

  componentDidMount() {
    this.getPageData();
  },

  getPageData() {
    const {userID} = this.props.params;
    showUser(userID).then(({data}) => {
      this.setState({user: data});
    });
  },

  handleSubmit(e) {
    e.preventDefault();
    const email = this.email.value;
    const lastName = this.lastName.value;
    const firstName = this.firstName.value;
    const admin = this.admin.checked;
    const password = this.newPassword.value;
    const confirmation = this.newPasswordConfirmation.value;
    const newUser = Object.assign({}, this.state.user, {lastName, firstName, email, admin, password, confirmation});

    updateUser(this.props.params.userID, newUser).then(() => {
      this.props.addFlashMessage({
        text: 'User updated successfully',
        type: 'success',
      });

      this.newPassword.value = '';
      this.newPasswordConfirmation.value = '';
    }).catch((err) => {
      this.props.addFlashMessage({
        text: err.toString(),
        type: 'error',
      });
    });
  },

  handleAddClusterLinks(clusterLinks) {
    this.setState({clusterLinks});
  },

  handleCreateClusterLinks() {
    const {user} = this.state;
    const clusterLinks = this.getClusterLinks();
    batchCreateUserClusterLink(
      user.id,
      clusterLinks,
    ).then(() => {
      this.props.addFlashMessage({
        type: 'success',
        text: `Cluster account added to ${user.name}`,
      });
      this.getPageData();
    }).catch((err) => {
      this.props.addFlashMessage({
        type: 'error',
        text: 'Something went wrong while trying to add cluster account',
      });
      console.error(err); // eslint-disable-line no-console
    });
  },

  handleDissociation() {
    const {linkToDissociate, user} = this.state;
    const {params: {clusterID}} = this.props;
    deleteUserClusterLink(clusterID, linkToDissociate.id).then(() => {
      this.props.addFlashMessage({
        text: `${user.name} dissociated from ${linkToDissociate.cluster_user}`,
        type: 'success',
      });
      this.getPageData();
    }).catch(() => {
      this.props.addFlashMessage({
        text: `Something went wrong`,
        type: 'error',
      });
    });
  },

  handleLinkToDissociate(linkToDissociate) {
    this.setState({linkToDissociate});
  },

  getClusterLinks() {
    return Object.keys(this.state.clusterLinks).map((clusterID) => {
      return {
        cluster_id: clusterID,
        cluster_user: this.state.clusterLinks[clusterID],
      };
    });
  },

  render() {
    const {user, linkToDissociate} = this.state;
    if (!user) {
      return null; // or something else, idk
    }

    return (
      <div>
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>
                {user.name}
                &nbsp;<span className="label label-primary">Web User</span>
              </h1>
            </div>
            <div className="enterprise-header__right">
              <button data-toggle="modal" data-target="#addClusterAccountModal" className="btn btn-sm btn-primary">Link Cluster Accounts</button>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-md-12">
              <div className="panel panel-minimal">
                <div className="panel-heading">
                  <h2 className="panel-title">Account Details</h2>
                </div>
                <div className="panel-body">
                  <form onSubmit={this.handleSubmit}>
                    <div className="form-group users-header__select-all">
                      <input ref={(ref) => this.admin = ref} type="checkbox" id="users-header__select-all" defaultChecked={user.admin} />
                      <label htmlFor="users-header__select-all" id="select-all-subtext">Admin</label>
                    </div>
                    <div className="form-group col-sm-6">
                      <label htmlFor="account-first-name">First Name</label>
                      <input ref={(ref) => this.firstName = ref} id="account-first-name" type="text" className="form-control" defaultValue={user.first_name} />
                    </div>
                    <div className="form-group col-sm-6">
                      <label htmlFor="account-last-name">Last Name</label>
                      <input ref={(ref) => this.lastName = ref} id="account-last-name" type="text" className="form-control" defaultValue={user.last_name} />
                    </div>
                    <div className="form-group col-sm-12">
                      <label htmlFor="account-email">Email Address</label>
                      <input ref={(ref) => this.email = ref} id="account-email" type="text" className="form-control" defaultValue={user.email} placeholder="Enter an email address" name="email" />
                    </div>
                    <div className="form-group col-sm-6">
                      <label htmlFor="account-new-password">New Password</label>
                      <input ref={(ref) => this.newPassword = ref} id="account-new-password" type="password" className="form-control" name="password" />
                    </div>
                    <div className="form-group col-sm-6">
                      <label htmlFor="account-confirm-new-password">Confirm New Password</label>
                      <input ref={(ref) => this.newPasswordConfirmation = ref} id="account-confirm-new-password" type="password" className="form-control" name="confirmation" />
                    </div>
                    <div className="form-group col-sm-12 u-flex u-jc-center">
                      <input type="submit" className="btn btn-primary btn-sm" value="Update User" />
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>
          <div className="row">
            <div className="col-md-12">
              <div className="panel panel-minimal">
                <div className="panel-heading u-flex u-jc-space-between u-ai-center">
                  <h2 className="panel-title">Linked Cluster Accounts</h2>
                </div>
                <div className="panel-body">
                  {this.renderClusterLinks()}
                </div>
              </div>
            </div>
          </div>
        </div>
        <AddClusterLinksModal user={user} onCreateClusterLinks={this.handleCreateClusterLinks} onAddClusterAccount={this.handleAddClusterLinks} />
        <DissociateModal user={user} clusterLink={linkToDissociate} handleDissociate={this.handleDissociation} />
      </div>
    );
  },

  renderClusterLinks() {
    const {user: {cluster_links}} = this.state;
    if (!cluster_links.length) {
      return (
        <div className="generic-empty-state">
          <span className="icon user-outline"/>
          <h4>No Cluster Accounts linked to this user</h4>
        </div>
      );
    }

    return (
      <table className="table v-center users-table">
        <tbody>
          <tr>
            <th></th>
            <th>Account</th>
            <th>Cluster</th>
            <th></th>
          </tr>
          {
            cluster_links.map((link) => {
              return (
                <tr key={link.id}>
                  <td></td>
                  <td>{link.cluster_user}</td>
                  <td>{link.cluster_id}</td>
                  <td>
                    <button
                      onClick={() => this.handleLinkToDissociate(link)}
                      type="button"
                      className="btn btn-sm btn-link-danger"
                      data-toggle="modal"
                      data-target="#dissociateLink"
                    >
                    Unlink
                    </button>
                  </td>
                </tr>
              );
            })
          }
        </tbody>
      </table>
    );
  },
});

export default FlashMessages(WebUserEdit);
