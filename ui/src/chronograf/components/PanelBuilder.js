import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import classNames from 'classnames';
import {bindActionCreators} from 'redux';
import ExplorerList from './ExplorerList';
import RawQueryList from './RawQueryList';
import * as viewActions from '../actions/view';

const EXPLORER = 'explorer';
const RAW_QUERY = 'raw query';

const {string, func} = PropTypes;
const PanelBuilder = React.createClass({
  propTypes: {
    width: string,
    actions: PropTypes.shape({
      createPanel: func.isRequired,
      deleteQuery: func.isRequired,
      addQuery: func.isRequired,
      chooseNamespace: func.isRequired,
      chooseMeasurement: func.isRequired,
      toggleField: func.isRequired,
      groupByTime: func.isRequired,
      applyFuncsToField: func.isRequired,
      chooseTag: func.isRequired,
      groupByTag: func.isRequired,
      toggleTagAcceptance: func.isRequired,
      deletePanel: func.isRequired,
    }).isRequired,
    setActivePanel: func.isRequired,
    activePanelID: string,
  },

  getInitialState() {
    return {
      activeTab: EXPLORER,
    };
  },

  handleClickTab(nextTab) {
    this.setState({
      activeTab: nextTab,
      activeQueryID: null,
    });
  },

  handleCreateExploer() {
    this.props.actions.createPanel();
  },

  handleClickStatement(queryID) {
    this.setState({
      activeTab: RAW_QUERY,
      activeQueryID: queryID,
    });
  },

  render() {
    const {width} = this.props;
    const {activeTab} = this.state;

    return (
      <div className="panel-builder" style={{width}}>
        <div className="panel-builder__tabs">
          <div className={classNames("panel-builder__tab", {active: activeTab === EXPLORER})} onClick={() => this.handleClickTab(EXPLORER)}>Explorer</div>
          <div className={classNames("panel-builder__tab", {active: activeTab === RAW_QUERY})} onClick={() => this.handleClickTab(RAW_QUERY)}>Raw Query</div>
        </div>
        <div className="panel-builder__tab-content">
          <div className="panel-builder__item btn btn-block btn-primary" onClick={this.handleCreateExploer}><span className="icon graphline"></span>&nbsp;&nbsp;Create Graph</div>
          {this.renderTab()}
        </div>
      </div>
    );
  },

  renderTab() {
    const {actions} = this.props;
    const {activeTab} = this.state;

    if (activeTab === EXPLORER) {
      return (
        <ExplorerList
          actions={actions}
          onClickStatement={this.handleClickStatement}
          setActivePanel={this.props.setActivePanel}
          activePanelID={this.props.activePanelID}
        />
      );
    }

    return (
      <RawQueryList
        actions={actions}
        activeQueryID={this.state.activeQueryID}
        activePanelID={this.props.activePanelID}
        setActivePanel={this.props.setActivePanel}
      />
    );
  },
});

function mapStateToProps() {
  return {};
}

function mapDispatchToProps(dispatch) {
  return {
    actions: bindActionCreators(viewActions, dispatch),
  };
}

export default connect(mapStateToProps, mapDispatchToProps)(PanelBuilder);
