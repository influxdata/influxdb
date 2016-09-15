import React from 'react';
import errorCopy from 'hson!shared/copy/errors.hson';

const NoClusterError = React.createClass({
  render() {
    return (
      <div>
        <div className="container">
          <div className="row">
            <div className="col-sm-6 col-sm-offset-3">
              <div className="panel panel-error panel-summer">
                <div className="panel-heading text-center">
                  <h2 className="deluxe">
                    {errorCopy.noCluster.head}
                  </h2>
                </div>
                <div className="panel-body text-center">
                  <h3 className="deluxe">How to resolve:</h3>
                  <p>
                    {errorCopy.noCluster.body}
                  </p>
                  <div className="text-center">
                    <button className="btn btn-center btn-success" onClick={() => window.location.reload()}>My Cluster Is Back Up</button>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default NoClusterError;
