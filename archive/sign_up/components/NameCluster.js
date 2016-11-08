import React, {PropTypes} from 'react';

const NameCluster = React.createClass({
  propTypes: {
    onNameCluster: PropTypes.func.isRequired,
  },

  handleSubmit(e) {
    e.preventDefault();
    this.props.onNameCluster(this.clusterName.value);
  },

  render() {
    return (
      <div id="signup-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-heading text-center">
                  <div className="signup-progress-circle step1of3">1/3</div>
                  <h2 className="deluxe">Welcome to InfluxEnterprise</h2>
                  <p>
                  </p>
                </div>
                <div className="panel-body">
                  <form onSubmit={this.handleSubmit}>
                    <div className="form-group col-sm-12">
                      <h4>What do you want to call your cluster?</h4>
                      <label htmlFor="cluster-name">Cluster Name (you can change this later)</label>
                      <input ref={(name) => this.clusterName = name} className="form-control input-lg" type="text" id="cluster-name" placeholder="Ex. MyCluster"/>
                    </div>

                    <div className="form-group col-sm-6 col-sm-offset-3">
                      <button className="btn btn-lg btn-block btn-success" type="submit">Next</button>
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

export default NameCluster;
