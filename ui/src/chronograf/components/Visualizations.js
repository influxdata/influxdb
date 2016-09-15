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
  },

  render() {
    const {panels, queryConfigs, timeRange, width, activePanelID} = this.props;

    const visualizations = Object.keys(panels).map((panelID) => {
      const panel = panels[panelID];
      const queries = panel.queryIds.map((id) => queryConfigs[id]);
      return <Visualization name={panel.name} key={panelID} queryConfigs={queries} timeRange={timeRange} isActive={panelID === activePanelID} />;
    });

    return (
      <div className="panels" style={{width}}>
        {visualizations}
      </div>
    );
  },
});

function mapStateToProps(state) {
  return {
    panels: state.panels,
    queryConfigs: state.queryConfigs,
  };
}

export default connect(mapStateToProps)(Visualizations);
