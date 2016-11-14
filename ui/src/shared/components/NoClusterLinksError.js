import React from 'react';

const NoClusterLinksError = React.createClass({
  render() {
    return (
      <div className="container-fluid">
        <div className="row">
          <div className="col-sm-6 col-sm-offset-3">
            <div className="panel panel-error panel-summer">
              <div className="panel-heading text-center">
                <h2 className="deluxe">
                  This user is not associated with any cluster accounts!
                </h2>
              </div>
              <div className="panel-body text-center">
                <p>Many features in Chronograf require your user to be associated with a cluster account.</p>
                <p>Ask an administrator to associate your user with a cluster account.</p>
              </div>
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default NoClusterLinksError;
