import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import _ from 'lodash';

import Panel from './Panel';

const {func, string, shape} = PropTypes;
const PanelList = React.createClass({
  propTypes: {
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    panels: shape({}).isRequired,
    queryConfigs: PropTypes.shape({}),
    actions: shape({
      activatePanel: func.isRequired,
      deleteQuery: func.isRequired,
      addQuery: func.isRequired,
    }).isRequired,
    setActiveQuery: func.isRequired,
    activePanelID: string,
    activeQueryID: string,
  },

  handleTogglePanel(panel) {
    const panelID = panel.id === this.props.activePanelID ? null : panel.id;
    this.props.actions.activatePanel(panelID);

    // Reset the activeQueryID when toggling Exporations
    this.props.setActiveQuery(null);
  },

  render() {
    const {actions, panels, timeRange, queryConfigs, setActiveQuery, activeQueryID, activePanelID} = this.props;

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
            <Panel
              key={panelID}
              panel={panel}
              queries={queries}
              timeRange={timeRange}
              onTogglePanel={this.handleTogglePanel}
              setActiveQuery={setActiveQuery}
              isExpanded={panelID === activePanelID}
              actions={allActions}
              activeQueryID={activeQueryID}
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

export default connect(mapStateToProps)(PanelList);
