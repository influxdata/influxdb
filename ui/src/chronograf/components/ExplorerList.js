import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import _ from 'lodash';

import Explorer from './Explorer';

const {func, string, shape} = PropTypes;
const ExplorerList = React.createClass({
  propTypes: {
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    panels: shape({}).isRequired,
    queryConfigs: PropTypes.shape({}),
    actions: shape({}).isRequired,
    onClickStatement: func.isRequired,
    setActivePanel: func.isRequired,
    activePanelID: string,
  },

  handleToggleExplorer(panel) {
    // If the explorer being toggled is currently active, it means we should
    // close everything by setting `activeExplorerIndex` to null.
    const activePanelID = panel.id === this.props.activePanelID ?
      null : panel.id;

    this.props.setActivePanel(activePanelID);
  },

  render() {
    const {actions, panels, timeRange, queryConfigs} = this.props;

    const activePanelID = this.props.activePanelID;

    return (
      <div>
        {Object.keys(panels).map((panelID) => {
          const panel = panels[panelID];
          const queries = panel.queryIds.map((configId) => queryConfigs[configId]);
          const deleteQueryFromPanel = _.partial(actions.deleteQuery, panelID);
          const addQueryToPanel = _.partial(actions.addQuery, panelID);
          const allActions = Object.assign({}, actions, {
            addQuery: addQueryToPanel,
            deleteQuery: deleteQueryFromPanel,
          });

          return (
            <Explorer
              key={panelID}
              panel={panel}
              queries={queries}
              timeRange={timeRange}
              onToggleExplorer={this.handleToggleExplorer}
              isExpanded={panelID === activePanelID}
              actions={allActions}
              onClickStatement={this.props.onClickStatement}
            />
          );
        })}
      </div>
    );
  },
});

function mapStateToProps(state) {
  return {
    timeRange: state.timeRange,
    panels: state.panels,
    queryConfigs: state.queryConfigs,
  };
}

export default connect(mapStateToProps)(ExplorerList);
