import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import PanelBuilder from '../components/PanelBuilder';
import Visualizations from '../components/Visualizations';
import Header from '../containers/Header';
import ResizeContainer from 'shared/components/ResizeContainer';

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
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    activePanel: string,
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
    const {timeRange, setTimeRange, activePanel} = this.props;

    return (
      <div className="data-explorer">
        <Header
          actions={{setTimeRange}}
          timeRange={timeRange}
        />
        <ResizeContainer>
          <PanelBuilder
            timeRange={timeRange}
            activePanelID={activePanel}
            activeQueryID={this.state.activeQueryID}
            setActiveQuery={this.handleSetActiveQuery}
          />
          <Visualizations
            timeRange={timeRange}
            activePanelID={activePanel}
            activeQueryID={this.state.activeQueryID}
          />
        </ResizeContainer>
      </div>
    );
  },
});

function mapStateToProps(state) {
  const {timeRange, dataExplorerUI} = state;

  return {
    timeRange,
    activePanel: dataExplorerUI.activePanel,
  };
}

export default connect(mapStateToProps, {
  setTimeRange: setTimeRangeAction,
})(DataExplorer);
