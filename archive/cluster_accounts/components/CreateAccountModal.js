import React, {PropTypes} from 'react';

const CreateAccountModal = React.createClass({
  propTypes: {
    onCreateAccount: PropTypes.func.isRequired,
    roles: PropTypes.arrayOf(PropTypes.shape({
      name: PropTypes.shape,
    })),
  },

  handleConfirm(e) {
    e.preventDefault();

    const name = this.name.value;
    const password = this.password.value;
    const role = this.accountRole.value;

    $('#createAccountModal').modal('hide'); // eslint-disable-line no-undef
    this.props.onCreateAccount(name, password, role);
  },

  render() {
    return (
      <div className="modal fade" id="createAccountModal" tabIndex="-1" role="dialog">
        <div className="modal-dialog">
          <div className="modal-content">
            <div className="modal-header">
              <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                <span aria-hidden="true">×</span>
              </button>
              <h4 className="modal-title">Create Cluster Account</h4>
            </div>
            <form onSubmit={this.handleConfirm} data-test="cluster-account-form">
              <div className="modal-body">
                <div className="row">
                  <div className="form-group col-xs-6">
                    <label htmlFor="account-name">Username</label>
                    <input ref={(r) => this.name = r} className="form-control" type="text" id="account-name" data-test="account-name" required={true} />
                  </div>
                  <div className="form-group col-xs-6">
                    <label htmlFor="account-password">Password</label>
                    <input ref={(r) => this.password = r} className="form-control" type="password" id="account-password" data-test="account-password" required={true} />
                  </div>
                </div>
                <div className="row">
                  <div className="form-group col-xs-6">
                    <label htmlFor="account-role">Role</label>
                    <select ref={(r) => this.accountRole = r} id="account-role" className="form-control input-lg">
                      {this.props.roles.map((r, i) => {
                        return <option key={i}>{r.name}</option>;
                      })}
                    </select>
                  </div>
                </div>
              </div>
              <div className="modal-footer">
                <button className="btn btn-default" type="button" data-dismiss="modal">Cancel</button>
                <button className="btn btn-danger js-delete-users" type="submit">Create Account</button>
              </div>
            </form>
          </div>
        </div>
      </div>
    );
  },
});

export default CreateAccountModal;
