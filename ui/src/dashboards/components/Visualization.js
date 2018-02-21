import React, {PropTypes} from 'react'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import buildQueries from 'utils/buildQueriesForGraphs'
import VisualizationName from 'src/dashboards/components/VisualizationName'

import {stringifyColorValues} from 'src/dashboards/constants/gaugeColors'

const DashVisualization = (
  {
    axes,
    type,
    colors,
    templates,
    timeRange,
    autoRefresh,
    queryConfigs,
    editQueryStatus,
    resizerTopHeight,
  },
  {source: {links: {proxy}}}
) =>
  <div className="graph">
    <VisualizationName />
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
      />
    </div>
  </div>

const {arrayOf, func, number, shape, string} = PropTypes

DashVisualization.defaultProps = {
  type: '',
}

DashVisualization.propTypes = {
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
}

DashVisualization.contextTypes = {
  source: PropTypes.shape({
    links: PropTypes.shape({
      proxy: PropTypes.string.isRequired,
    }).isRequired,
  }).isRequired,
}

export default DashVisualization
