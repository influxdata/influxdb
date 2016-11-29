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
      base64ExplorerID: PropTypes.string,
    }).isRequired,
  },

  componentDidMount() {
    const {base64ExplorerID} = this.props.params;
    this.props.fetchExplorers({
      source: this.props.source,
      userID: 1, // TODO: get the userID
      explorerID: base64ExplorerID ? this.decodeID(base64ExplorerID) : null,
      push: this.props.router.push,
    });
  },

  render() {
    const {base64ExplorerID} = this.props.params;

    return (
      <div className="page">
        <DataExplorer source={this.props.source} explorerID={this.decodeID(base64ExplorerID)} />
      </div>
    );
  },

  decodeID(base64Id) {
    try {
      return atob(base64Id);
    } catch (e) {
      if (!(e instanceof DOMException && e.name === "InvalidCharacterError")) {
        throw e;
      }

      return null;
    }
  },
});

function mapStateToProps() {
  return {};
}

export default connect(mapStateToProps, {
  fetchExplorers,
})(withRouter(App));
