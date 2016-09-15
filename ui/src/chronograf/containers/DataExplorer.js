import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import PanelBuilder from '../components/PanelBuilder';
import Visualizations from '../components/Visualizations';
import Header from '../containers/Header';
import ResizeContainer from 'shared/components/ResizeContainer';
import NotFound from 'shared/components/NotFound';
import {FETCHING} from '../reducers/explorers';
import {
  setTimeRange as setTimeRangeAction,
  createExplorer as createExplorerAction,
  chooseExplorer as chooseExplorerAction,
  deleteExplorer as deleteExplorerAction,
  editExplorer as editExplorerAction,
} from '../actions/view';

const DataExplorer = React.createClass({
  propTypes: {
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
    timeRange: PropTypes.shape({
      upper: PropTypes.string,
      lower: PropTypes.string,
    }).isRequired,
    explorers: PropTypes.shape({}).isRequired,
    explorerID: PropTypes.number.isRequired,
    clusterID: PropTypes.string.isRequired,
    setTimeRange: PropTypes.func.isRequired,
    createExplorer: PropTypes.func.isRequired,
    chooseExplorer: PropTypes.func.isRequired,
    deleteExplorer: PropTypes.func.isRequired,
    editExplorer: PropTypes.func.isRequired,
  },

  childContextTypes: {
    dataNodes: PropTypes.arrayOf(PropTypes.string.isRequired).isRequired,
  },

  getChildContext() {
    return {dataNodes: this.props.dataNodes};
  },

  getInitialState() {
    return {
      activePanelId: null,
    };
  },

  handleSetActivePanel(id) {
    this.setState({
      activePanelID: id,
    });
  },

  render() {
    const {timeRange, explorers, explorerID, clusterID, setTimeRange, createExplorer, chooseExplorer, deleteExplorer, editExplorer} = this.props;

    if (explorers === FETCHING) {
      // TODO: page-wide spinner
      return null;
    }

    const activeExplorer = explorers[explorerID];
    if (!activeExplorer) {
      return <NotFound />;
    }

    return (
      <div className="data-explorer">
        <Header
          actions={{setTimeRange, createExplorer, chooseExplorer, deleteExplorer, editExplorer}}
          explorers={explorers}
          timeRange={timeRange}
          explorerID={explorerID}
          clusterID={clusterID}
        />
        <ResizeContainer>
          <PanelBuilder timeRange={timeRange} activePanelID={this.state.activePanelID} setActivePanel={this.handleSetActivePanel} />
          <Visualizations clusterID={clusterID} timeRange={timeRange} activePanelID={this.state.activePanelID} />
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
  createExplorer: createExplorerAction,
  chooseExplorer: chooseExplorerAction,
  deleteExplorer: deleteExplorerAction,
  editExplorer: editExplorerAction,
})(DataExplorer);
