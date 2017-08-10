import React, {PropTypes} from 'react'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import buildQueries from 'utils/buildQueriesForGraphs'

const DashVisualization = (
  {
    axes,
    height,
    cellType,
    cellName,
    templates,
    timeRange,
    autoRefresh,
    queryConfigs,
    heightPixels,
    editQueryStatus,
  },
  {source: {links: {proxy}}}
) =>
  <div className="graph" style={{height}}>
    <div className="graph-heading">
      <div className="graph-title">
        {cellName}
      </div>
    </div>
    <div className="graph-container">
      <RefreshingGraph
        axes={axes}
        type={cellType}
        queries={buildQueries(proxy, queryConfigs, timeRange)}
        templates={templates}
        cellHeight={heightPixels}
        autoRefresh={autoRefresh}
        editQueryStatus={editQueryStatus}
      />
    </div>
  </div>

const {arrayOf, func, number, shape, string} = PropTypes

DashVisualization.defaultProps = {
  cellName: '',
  cellType: '',
}

DashVisualization.propTypes = {
  cellName: string,
  cellType: string,
  autoRefresh: number.isRequired,
  templates: arrayOf(shape()),
  timeRange: shape({
    upper: string,
    lower: string,
  }).isRequired,
  queryConfigs: arrayOf(shape({})).isRequired,
  height: string,
  heightPixels: number,
  editQueryStatus: func.isRequired,
  axes: shape({
    y: shape({
      bounds: arrayOf(string),
    }),
  }),
}

DashVisualization.contextTypes = {
  source: PropTypes.shape({
    links: PropTypes.shape({
      proxy: PropTypes.string.isRequired,
    }).isRequired,
  }).isRequired,
}

export default DashVisualization
