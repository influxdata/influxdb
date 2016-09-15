import React, {PropTypes} from 'react';
import {getClusters} from 'shared/apis';
import FlashMessages from 'shared/components/FlashMessages';
import ClusterAccounts from 'shared/components/AddClusterAccounts';
import $ from 'jquery';

const {string, func} = PropTypes;
const InviteUserModal = React.createClass({
  propTypes: {
    clusterID: string.isRequired,
    addFlashMessage: func.isRequired,
    onCreateWebUser: func.isRequired,
    onSelectClusterAccount: func.isRequired,
  },

  getInitialState() {
    return {
      errors: null,
      clusters: [],
      clusterLinks: {},
    };
  },

  componentDidMount() {
    getClusters().then(({data}) => {
      this.setState({clusters: data});
    });
  },

  handleSubmit(e) {
    e.preventDefault();
    const firstName = this.firstName.value;
    const lastName = this.lastName.value;
    const email = this.email.value;
    const password = this.password.value;

    this.props.onCreateWebUser({clusterID: this.props.clusterID, firstName, lastName, email, password});
    this.cleanup();
  },

  render() {
    return (
      <div className="modal fade" id="createUserModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Invite User</h4>
            </div>
            <form onSubmit={this.handleSubmit}>
              <div className="modal-body">
                <div className="form-grid">
                  <div className="form-group col-md-12">
                    <div className="alert alert-info">
                      <span className="icon star"></span>
                      Please enter first and last name followed by their email they will use to login
                    </div>
                  </div>
                  {this.state.errors ?
                    <div className="form-group col-md-12">
                      <ul className="alert alert-danger">
                        {this.state.errors.map((error, i) => <li key={i}>{error}</li>)}
                      </ul>
                    </div>
                    : null
                  }
                  <div className="form-group col-md-6">
                    <label htmlFor="firstName">First Name</label>
                    <input ref={(ref) => this.firstName = ref} type="text" className="form-control" id="firstName" placeholder="Marty" required={true} />
                  </div>
                  <div className="form-group col-md-6">
                    <label htmlFor="lastName">Last Name</label>
                    <input ref={(ref) => this.lastName = ref} type="text" className="form-control" id="lastName" placeholder="McFly" required={true} />
                  </div>
                  <div className="form-group col-md-12">
                    <label htmlFor="userEmail">Email</label>
                    <input ref={(ref) => this.email = ref} type="email" className="form-control" id="userEmail" placeholder="marty@thepinheadsrock.com" required={true} />
                  </div>
                  <div className="form-group col-md-12">
                    <label htmlFor="userPassword">Password</label>
                    <input ref={(ref) => this.password = ref} type="password" className="form-control" id="userPassword" placeholder="Optional Password"></input>
                  </div>
                </div>
                <ClusterAccounts onSelectClusterAccount={this.props.onSelectClusterAccount} clusters={this.state.clusters}/>
              </div>
              <div className="modal-footer">
                <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
                <input className="btn btn-success" type="submit" value="Invite"></input>
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },

  cleanup() {
    $('.modal').hide();
    $('.modal-backdrop').hide();

    // reset form values
    this.firstName.value = '';
    this.lastName.value = '';
    this.email.value = '';
    this.password.value = '';
  },
});

export default FlashMessages(InviteUserModal);
