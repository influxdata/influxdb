import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import {withRouter} from 'react-router';
import {fetchExplorers} from '../actions/view';
import DataExplorer from './DataExplorer';

const App = React.createClass({
  propTypes: {
    proxyLink: PropTypes.string.isRequired,
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
      sourceLink: 'linkgoesheres', // source.links.self
      userID: 1, // userID
      explorerID: Number(explorerID),
      push: this.props.router.push,
    });
  },

  render() {
    const {explorerID} = this.props.params;
    return (
      <div className="data-explorer-container">
        <DataExplorer proxyLink={this.props.proxyLink} explorerID={Number(explorerID)} />
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
