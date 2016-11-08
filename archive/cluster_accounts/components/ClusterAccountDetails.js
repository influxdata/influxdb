import React, {PropTypes} from 'react';

const {string, func, bool} = PropTypes;

const ClusterAccountDetails = React.createClass({
  propTypes: {
    name: string.isRequired,
    onUpdatePassword: func.isRequired,
    showDelete: bool,
  },

  getDefaultProps() {
    return {
      showDelete: true,
    };
  },

  getInitialState() {
    return {
      passwordsMatch: true,
    };
  },

  handleSubmit(e) {
    e.preventDefault();
    const password = this.password.value;
    const confirmation = this.confirmation.value;
    const passwordsMatch = password === confirmation;
    if (!passwordsMatch) {
      return this.setState({passwordsMatch});
    }

    this.props.onUpdatePassword(password);
  },

  render() {
    return (
      <div id="settings-page">
        <div className="panel panel-default">
          <div className="panel-body">
            <form onSubmit={this.handleSubmit}>
              {this.renderPasswordMismatch()}
              <div className="form-group col-sm-12">
                <label htmlFor="name">Name</label>
                <input disabled={true} className="form-control input-lg" type="name" id="name" name="name" value={this.props.name}/>
              </div>
              <div className="form-group col-sm-6">
                <label htmlFor="password">Password</label>
                <input ref={(password) => this.password = password} className="form-control input-lg" type="password" id="password" name="password"/>
              </div>
              <div className="form-group col-sm-6">
                <label htmlFor="password-confirmation">Confirm Password</label>
                <input ref={(confirmation) => this.confirmation = confirmation} className="form-control input-lg" type="password" id="password-confirmation" name="confirmation"/>
              </div>
              <div className="form-group col-sm-6 col-sm-offset-3">
                <button className="btn btn-next btn-success btn-lg btn-block" type="submit">Reset Password</button>
              </div>
            </form>
          </div>
        </div>
        {this.props.showDelete ? (
          <div className="panel panel-default delete-account">
            <div className="panel-body">
              <div className="col-sm-3">
                <button
                  className="btn btn-next btn-danger btn-lg"
                  type="submit"
                  data-toggle="modal"
                  data-target="#deleteClusterAccountModal">
                  Delete Account
                </button>
              </div>
              <div className="col-sm-9">
                <h4>Delete this cluster account</h4>
                <p>Beware! We won't be able to recover a cluster account once you've deleted it.</p>
              </div>
            </div>
          </div>
        ) : null}
      </div>
    );
  },

  renderPasswordMismatch() {
    if (this.state.passwordsMatch) {
      return null;
    }

    return <div>Passwords do not match</div>;
  },
});

export default ClusterAccountDetails;
