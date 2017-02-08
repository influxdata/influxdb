import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import QueryBuilder from '../components/QueryBuilder';
import Visualization from '../components/Visualization';
import Header from '../containers/Header';

import {
  setTimeRange as setTimeRangeAction,
} from '../actions/view';

const {
  func,
  shape,
  string,
} = PropTypes;

const DataExplorer = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
      }).isRequired,
    }).isRequired,
    queryConfigs: PropTypes.shape({}),
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    setTimeRange: func.isRequired,
  },

  childContextTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
        self: string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getChildContext() {
    return {source: this.props.source};
  },

  getInitialState() {
    return {
      activeQueryID: null,
    };
  },

  handleSetActiveQuery(id) {
    this.setState({activeQueryID: id});
  },

  render() {
    const {timeRange, setTimeRange, queryConfigs} = this.props;
    const {activeQueryID} = this.state;
    const queries = Object.keys(queryConfigs).map((q) => queryConfigs[q]);

    return (
      <div className="data-explorer">
        <Header
          actions={{setTimeRange}}
          timeRange={timeRange}
        />
        <div className="page-contents">
          <Visualization
            timeRange={timeRange}
            queryConfigs={queries}
            activeQueryID={this.state.activeQueryID}
            activeQueryIndex={0}
          />
          <QueryBuilder
            queries={queries}
            timeRange={timeRange}
            setActiveQuery={this.handleSetActiveQuery}
            activeQueryID={activeQueryID}
          />
        </div>
      </div>
    );
  },
});

function mapStateToProps(state) {
  const {timeRange, queryConfigs, dataExplorerUI} = state;

  return {
    timeRange,
    queryConfigs,
    activePanel: dataExplorerUI.activePanel,
  };
}

export default connect(mapStateToProps, {
  setTimeRange: setTimeRangeAction,
})(DataExplorer);
