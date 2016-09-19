import React from 'react';
import ClusterError from './ClusterError';

const InsufficientPermissions = React.createClass({
  render() {
    return (
      <ClusterError>
        <div className="panel-heading text-center">
          <h2 className="deluxe">
            {`Your account has insufficient permissions`}
          </h2>
        </div>
        <div className="panel-body text-center">
          <h3 className="deluxe">Talk to your admin to get additional permissions for access</h3>
        </div>
      </ClusterError>
    );
  },
});

export default InsufficientPermissions;
