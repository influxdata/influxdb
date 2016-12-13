import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import Visualization from './Visualization';

const {shape, string} = PropTypes;

const Visualizations = React.createClass({
  propTypes: {
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    panels: shape({}).isRequired,
    queryConfigs: shape({}).isRequired,
    width: string,
    activePanelID: string,
    activeQueryID: string,
  },

  render() {
    const {panels, queryConfigs, timeRange, width, activePanelID} = this.props;

    const visualizations = Object.keys(panels).map((panelID) => {
      const panel = panels[panelID];
      const queries = panel.queryIds.map((id) => queryConfigs[id]);
      const isActive = panelID === activePanelID;

      return <Visualization activeQueryIndex={this.getActiveQueryIndex(panelID)} name={panel.name} key={panelID} queryConfigs={queries} timeRange={timeRange} isActive={isActive} />;
    });

    return (
      <div className="panels" style={{width}}>
        {visualizations}
      </div>
    );
  },

  getActiveQueryIndex(panelID) {
    const {activeQueryID, activePanelID, panels} = this.props;
    const isPanelActive = panelID === activePanelID;

    if (!isPanelActive) {
      return -1;
    }

    if (activeQueryID === null) {
      return 0;
    }

    return panels[panelID].queryIds.indexOf(activeQueryID);
  },
});

function mapStateToProps(state) {
  return {
    panels: state.panels,
    queryConfigs: state.queryConfigs,
  };
}

export default connect(mapStateToProps)(Visualizations);
