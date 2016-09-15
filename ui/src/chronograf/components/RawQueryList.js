import React, {PropTypes} from 'react';
import {connect} from 'react-redux';
import RawQueryGroup from './RawQueryGroup';

const {func, string, shape} = PropTypes;
const RawQueryList = React.createClass({
  propTypes: {
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    panels: shape({}).isRequired,
    queryConfigs: shape({}).isRequired,
    actions: shape({
      updateRawQuery: func.isRequired,
      renamePanel: func.isRequired,
    }).isRequired,
    activeQueryID: PropTypes.string,
    setActivePanel: PropTypes.func,
  },

  render() {
    const {panels, timeRange, queryConfigs, actions, setActivePanel, activeQueryID} = this.props;

    return (
      <div>
        <div className="alert alert-rawquery">
          <span className="icon alert-triangle"></span>
          Editing a query turns it into a <strong>Raw Query</strong> which is no longer editable from <strong>Explorer</strong> mode. This action cannot be undone.
        </div>
        {Object.keys(panels).map((panelID) => {
          const panel = panels[panelID];
          const queries = panel.queryIds.map((configId) => queryConfigs[configId]);
          return (
            <RawQueryGroup setActivePanel={setActivePanel} activeQueryID={activeQueryID} key={panelID} panel={panel} queryConfigs={queries} timeRange={timeRange} actions={actions} />
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

export default connect(mapStateToProps)(RawQueryList);
