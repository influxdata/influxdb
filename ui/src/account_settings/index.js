import React, {PropTypes} from 'react';
import {meShow, meUpdate} from 'shared/apis';
import FlashMessages from 'shared/components/FlashMessages';

const {func} = PropTypes;
const AccountSettingsPage = React.createClass({
  propTypes: {
    addFlashMessage: func.isRequired,
  },

  getInitialState() {
    return {};
  },

  componentDidMount() {
    meShow().then(({data: user}) => {
      this.setState({user});
    });
  },

  handleSubmit(e) {
    e.preventDefault();
    const email = this.email.value;
    const lastName = this.lastName.value;
    const firstName = this.firstName.value;
    const newUser = Object.assign({}, this.state.user, {lastName, firstName, email});
    meUpdate(newUser).then(() => {
      this.props.addFlashMessage({
        text: 'User updated successfully',
        type: 'success',
      });
    }).catch((err) => {
      this.props.addFlashMessage({
        text: err.toString(),
        type: 'error',
      });
    });
  },

  handleChangePassword(e) {
    e.preventDefault();
    const password = this.newPassword.value;
    const confirmation = this.newPasswordConfirmation.value;
    const newUser = Object.assign({}, this.state.user, {password, confirmation});
    meUpdate(newUser).then(() => {
      this.props.addFlashMessage({
        text: 'Password updated successfully',
        type: 'success',
      });

      // reset the form fields
      this.newPassword.value = '';
      this.newPasswordConfirmation.value = '';
    }).catch(() => {
      this.props.addFlashMessage({
        text: 'Passwords did not match',
        type: 'error',
      });
    });
  },

  render() {
    const {user} = this.state;
    if (!user) {
      return null; // or something else, idk
    }

    return (
      <div>
        <div className="enterprise-header">
          <div className="enterprise-header__container">
            <div className="enterprise-header__left">
              <h1>Account Settings</h1>
            </div>
          </div>
        </div>

        <div className="container-fluid">
          <div className="row">
            <div className="col-xs-12">
              <div className="panel panel-minimal">
                <div className="panel-heading">
                  <h2 className="panel-title">Personal Info</h2>
                </div>
                <div className="panel-body">
                  <form onSubmit={this.handleSubmit}>
                    <input type="hidden" />
                    <div className="form-group col-sm-4">
                      <label htmlFor="account-first-name">First Name</label>
                      <input ref={(ref) => this.firstName = ref} id="account-first-name" type="text" className="form-control" defaultValue={user.first_name} />
                    </div>
                    <div className="form-group col-sm-4">
                      <label htmlFor="account-last-name">Last Name</label>
                      <input ref={(ref) => this.lastName = ref} id="account-last-name" type="text" className="form-control" defaultValue={user.last_name} />
                    </div>
                    <div className="form-group col-sm-4">
                      <label htmlFor="account-email">Email Address</label>
                      <input ref={(ref) => this.email = ref} id="account-email" type="text" className="form-control" defaultValue={user.email} placeholder="Enter an email address" name="email" />
                    </div>
                    <div className="form-group col-sm-12">
                      <input type="submit" className="btn btn-primary" value="Change Info" />
                    </div>
                  </form>
                </div>
              </div>
              <div className="panel panel-minimal">
                <div className="panel-heading">
                  <h2 className="panel-title">Password</h2>
                </div>
                <div className="panel-body">
                  <form onSubmit={this.handleChangePassword}>
                    <input type="hidden" />
                    <div className="form-group col-sm-6">
                      <label htmlFor="account-new-password">New Password</label>
                      <input ref={(ref) => this.newPassword = ref} id="account-new-password" type="password" className="form-control" name="password" />
                    </div>
                    <div className="form-group col-sm-6">
                      <label htmlFor="account-confirm-new-password">Confirm New Password</label>
                      <input ref={(ref) => this.newPasswordConfirmation = ref} id="account-confirm-new-password" type="password" className="form-control" name="confirmation" />
                    </div>
                    <div className="form-group col-sm-12">
                      <input type="submit" className="btn btn-primary" value="Change Password" />
                    </div>
                  </form>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default FlashMessages(AccountSettingsPage);
