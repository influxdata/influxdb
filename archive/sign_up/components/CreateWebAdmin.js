import React, {PropTypes} from 'react';
import ClusterAccounts from 'shared/components/AddClusterAccounts';
import {getClusters} from 'shared/apis';

const CreateWebAdmin = React.createClass({
  propTypes: {
    onCreateWebAdmin: PropTypes.func.isRequired,
  },

  getInitialState() {
    return {
      clusters: [],
      clusterLinks: {},
      passwordsMatch: true,
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
    const confirmation = this.confirmation.value;

    if (password !== confirmation) {
      return this.setState({passwordsMatch: false});
    }

    this.props.onCreateWebAdmin(firstName, lastName, email, password, confirmation, this.getClusterLinks());
  },

  handleSelectClusterAccount({clusterID, accountName}) {
    const clusterLinks = Object.assign({}, this.state.clusterLinks, {
      [clusterID]: accountName,
    });
    this.setState({
      clusterLinks,
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

  render() {
    const {clusters, passwordsMatch, clusterLinks} = this.state;
    return (
      <div id="signup-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-heading text-center">
                  <div className="signup-progress-circle step3of3">3/3</div>
                  <h2 className="deluxe">Welcome to InfluxEnterprise</h2>
                </div>
                <div className="panel-body">
                  {passwordsMatch ? null : this.renderValidationError()}
                  <h4>Create a Web Administrator user.</h4>
                  <h5>A Web Administrator has all web console permissions.</h5>
                  <p>
                    After filling out the form with your name, email, and password, assign yourself to the Cluster Administrator account that you
                    created in the previous step. This ensures that you have all web console permissions and all cluster permissions.
                  </p>
                  <form onSubmit={this.handleSubmit}>
                    <div className="row">
                      <div className="form-group col-sm-6">
                        <label htmlFor="first-name">First Name</label>
                        <input ref={(firstName) => this.firstName = firstName} className="form-control input-lg" type="text" id="first-name" required={true} />
                      </div>
                      <div className="form-group col-sm-6">
                        <label htmlFor="last-name">Last Name</label>
                        <input ref={(lastName) => this.lastName = lastName} className="form-control input-lg" type="text" id="last-name" required={true} />
                      </div>
                    </div>
                    <div className="row">
                      <div className="form-group col-sm-12">
                        <label htmlFor="email">Email</label>
                        <input ref={(email) => this.email = email} className="form-control input-lg" type="text" id="email" required={true} />
                      </div>
                    </div>
                    <div className="row">
                      <div className="form-group col-sm-6">
                        <label htmlFor="password">Password</label>
                        <input ref={(password) => this.password = password} className="form-control input-lg" type="password" id="password" required={true} />
                      </div>
                      <div className="form-group col-sm-6">
                        <label htmlFor="confirmation">Confirm Password</label>
                        <input ref={(confirmation) => this.confirmation = confirmation} className="form-control input-lg" type="password" id="confirmation" required={true} />
                      </div>
                    </div>

                    {clusters.length ? <ClusterAccounts clusters={clusters} onSelectClusterAccount={this.handleSelectClusterAccount} /> : null}

                    <div className="form-group col-sm-6 col-sm-offset-3">
                      <button disabled={!Object.keys(clusterLinks).length} className="btn btn-lg btn-success btn-block" type="submit">Enter App</button>
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
    return <div>Your passwords don't match!</div>;
  },
});

export default CreateWebAdmin;
