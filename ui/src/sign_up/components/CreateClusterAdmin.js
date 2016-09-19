import React, {PropTypes} from 'react';

const CreateClusterAdmin = React.createClass({
  propTypes: {
    onCreateClusterAdmin: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      passwordsMatch: true,
    };
  },

  handleSubmit(e) {
    e.preventDefault();
    const username = this.username.value;
    const password = this.password.value;
    const confirmation = this.confirmation.value;

    if (password !== confirmation) {
      return this.setState({
        passwordsMatch: false,
      });
    }

    this.props.onCreateClusterAdmin(username, password);
  },

  render() {
    const {passwordsMatch} = this.state;

    return (
      <div id="signup-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-heading text-center">
                  <div className="signup-progress-circle step2of3">2/3</div>
                  <h2 className="deluxe">Welcome to InfluxEnterprise</h2>
                </div>
                <div className="panel-body">
                  {passwordsMatch ? null : this.renderValidationError()}
                  <h4>Create a Cluster Administrator account.</h4>
                  <p>Users assigned to the Cluster Administrator account have all cluster permissions.</p>
                  <form onSubmit={this.handleSubmit}>
                    <div className="form-group col-sm-12">
                      <label htmlFor="username">Account Name</label>
                      <input ref={(username) => this.username = username} className="form-control input-lg" type="text" id="username" required={true} placeholder="Ex. ClusterAdmin"/>
                    </div>
                    <div className="form-group col-sm-6">
                      <label htmlFor="password">Password</label>
                      <input ref={(pass) => this.password = pass} className="form-control input-lg" type="password" id="password" required={true}/>
                    </div>
                    <div className="form-group col-sm-6">
                      <label htmlFor="confirmation">Confirm Password</label>
                      <input ref={(conf) => this.confirmation = conf} className="form-control input-lg" type="password" id="confirmation" required={true} />
                    </div>


                    <div className="form-group col-sm-6 col-sm-offset-3">
                      <button className="btn btn-lg btn-success btn-block" type="submit">Next</button>
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

  renderValidationError() {
    return <div>Your passwords don't match! Please make sure they match.</div>;
  },
});

export default CreateClusterAdmin;
