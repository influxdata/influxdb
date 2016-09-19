import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import {withRouter} from 'react-router';
import {fetchExplorers} from '../actions/view';
import DataExplorer from './DataExplorer';

const App = React.createClass({
  propTypes: {
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    fetchExplorers: PropTypes.func.isRequired,
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
    }).isRequired,
    params: PropTypes.shape({
      clusterID: PropTypes.string.isRequired,
      explorerID: PropTypes.string,
    }).isRequired,
  },

  componentDidMount() {
    const {clusterID, explorerID} = this.props.params;
    this.props.fetchExplorers({
      clusterID,
      explorerID: Number(explorerID),
      push: this.props.router.push,
    });
  },

  childContextTypes: {
    clusterID: PropTypes.string,
  },

  getChildContext() {
    return {
      clusterID: this.props.params.clusterID,
    };
  },

  render() {
    const {clusterID, explorerID} = this.props.params;
    return (
      <div className="data-explorer-container">
        <DataExplorer dataNodes={this.props.dataNodes} clusterID={clusterID} explorerID={Number(explorerID)} />
      </div>
    );
  },
});

function mapStateToProps() {
  return {};
}

export default connect(mapStateToProps, {
  fetchExplorers,
})(withRouter(App));
