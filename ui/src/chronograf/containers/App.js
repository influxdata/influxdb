import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import {withRouter} from 'react-router';
import {fetchExplorers} from '../actions/view';
import DataExplorer from './DataExplorer';

const App = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    fetchExplorers: PropTypes.func.isRequired,
    router: PropTypes.shape({
      push: PropTypes.func.isRequired,
    }).isRequired,
    params: PropTypes.shape({
      explorerID: PropTypes.string,
    }).isRequired,
  },

  componentDidMount() {
    const {explorerID} = this.props.params;
    this.props.fetchExplorers({
      sourceLink: this.props.source.links.self,
      userID: 1, // TODO: get the userID
      explorerID: Number(explorerID),
      push: this.props.router.push,
    });
  },

  render() {
    const {explorerID} = this.props.params;
    return (
      <div className="data-explorer-container">
        <DataExplorer source={this.props.source} explorerID={Number(explorerID)} />
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
