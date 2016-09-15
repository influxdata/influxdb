import React from 'react';

const NoCluster = React.createClass({
  handleSubmit() {
    window.location.reload();
  },

  render() {
    return (
      <div id="signup-page">
        <div className="container">
          <div className="row">
            <div className="col-md-8 col-md-offset-2">
              <div className="panel panel-summer">
                <div className="panel-heading text-center">
                  <h2 className="deluxe">Welcome to Enterprise</h2>
                  <p>
                    Looks like you don't have your cluster set up.
                  </p>
                </div>
                <div className="panel-body">
                  <button className="btn btn-lg btn-success btn-block" onClick={this.handleSubmit}>Try Again</button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default NoCluster;
