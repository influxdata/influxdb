import React, {PropTypes} from 'react'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import buildQueries from 'utils/buildQueriesForGraphs'
import VisualizationName from 'src/dashboards/components/VisualizationName'

import {stringifyColorValues} from 'src/dashboards/constants/gaugeColors'

const DashVisualization = (
  {
    axes,
    type,
    name,
    colors,
    templates,
    timeRange,
    autoRefresh,
    onCellRename,
    queryConfigs,
    editQueryStatus,
    resizerTopHeight,
    staticLegend,
  },
  {source: {links: {proxy}}}
) =>
  <div className="graph">
    <VisualizationName defaultName={name} onCellRename={onCellRename} />
    <div className="graph-container">
      <RefreshingGraph
        colors={stringifyColorValues(colors)}
        axes={axes}
        type={type}
        queries={buildQueries(proxy, queryConfigs, timeRange)}
        templates={templates}
        autoRefresh={autoRefresh}
        editQueryStatus={editQueryStatus}
        resizerTopHeight={resizerTopHeight}
        staticLegend={staticLegend}
      />
    </div>
  </div>

const {arrayOf, bool, func, number, shape, string} = PropTypes

DashVisualization.defaultProps = {
  name: '',
  type: '',
}

DashVisualization.propTypes = {
  name: string,
  type: string,
  autoRefresh: number.isRequired,
  templates: arrayOf(shape()),
  timeRange: shape({
    upper: string,
    lower: string,
  }).isRequired,
  queryConfigs: arrayOf(shape({})).isRequired,
  editQueryStatus: func.isRequired,
  axes: shape({
    y: shape({
      bounds: arrayOf(string),
    }),
  }),
  onCellRename: func,
  resizerTopHeight: number,
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: number.isRequired,
    })
  ),
  staticLegend: bool,
}

DashVisualization.contextTypes = {
  source: PropTypes.shape({
    links: PropTypes.shape({
      proxy: PropTypes.string.isRequired,
    }).isRequired,
  }).isRequired,
}

export default DashVisualization
