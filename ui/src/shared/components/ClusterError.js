import React from 'react';

const {node} = React.PropTypes;
const ClusterError = React.createClass({
  propTypes: {
    children: node.isRequired,
  },

  render() {
    return (
      <div className="container-fluid">
        <div className="row">
          <div className="col-sm-6 col-sm-offset-3">
            <div className="panel panel-error panel-summer">
              {this.props.children}
            </div>
          </div>
        </div>
      </div>
    );
  },
});

export default ClusterError;
