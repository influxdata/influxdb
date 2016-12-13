import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import PanelBuilder from '../components/PanelBuilder';
import Visualizations from '../components/Visualizations';
import Header from '../containers/Header';
import ResizeContainer from 'shared/components/ResizeContainer';
import {FETCHING} from '../reducers/explorers';
import {
  setTimeRange as setTimeRangeAction,
  createExploration as createExplorationAction,
  chooseExploration as chooseExplorationAction,
  deleteExplorer as deleteExplorerAction,
  editExplorer as editExplorerAction,
} from '../actions/view';

const DataExplorer = React.createClass({
  propTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
    explorers: PropTypes.shape({}).isRequired,
    explorerID: PropTypes.string,
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    setTimeRange: PropTypes.func.isRequired,
    createExploration: PropTypes.func.isRequired,
    chooseExploration: PropTypes.func.isRequired,
    deleteExplorer: PropTypes.func.isRequired,
    editExplorer: PropTypes.func.isRequired,
  },

  childContextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
        self: PropTypes.string.isRequired,
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
    const {timeRange, explorers, explorerID, setTimeRange, createExploration, chooseExploration, deleteExplorer, editExplorer} = this.props;

    if (explorers === FETCHING || !explorerID) {
      // TODO: page-wide spinner
      return null;
    }

    return (
      <div className="data-explorer">
        <Header
          actions={{setTimeRange, createExploration, chooseExploration, deleteExplorer, editExplorer}}
          explorers={explorers}
          timeRange={timeRange}
          explorerID={explorerID}
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
    explorers: state.explorers,
  };
}

export default connect(mapStateToProps, {
  setTimeRange: setTimeRangeAction,
  createExploration: createExplorationAction,
  chooseExploration: chooseExplorationAction,
  deleteExplorer: deleteExplorerAction,
  editExplorer: editExplorerAction,
})(DataExplorer);
