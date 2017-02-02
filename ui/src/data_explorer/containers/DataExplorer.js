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
      activePanelID: null,
      activeQueryID: null,
    };
  },

  handleSetActivePanel(id) {
    this.setState({activePanelID: id});
  },

  handleSetActiveQuery(id) {
    this.setState({activeQueryID: id});
  },

  render() {
    const {timeRange, setTimeRange} = this.props;

    return (
      <div className="data-explorer">
        <Header
          actions={{setTimeRange}}
          timeRange={timeRange}
        />
        <ResizeContainer>
          <PanelBuilder
            timeRange={timeRange}
            activePanelID={this.state.activePanelID}
            activeQueryID={this.state.activeQueryID}
            setActiveQuery={this.handleSetActiveQuery}
            setActivePanel={this.handleSetActivePanel}
          />
          <Visualizations
            timeRange={timeRange}
            activePanelID={this.state.activePanelID}
            activeQueryID={this.state.activeQueryID}
          />
        </ResizeContainer>
      </div>
    );
  },
});

function mapStateToProps(state) {
  return {
    timeRange: state.timeRange,
  };
}

export default connect(mapStateToProps, {
  setTimeRange: setTimeRangeAction,
})(DataExplorer);
