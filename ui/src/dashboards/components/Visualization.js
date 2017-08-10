import React, {PropTypes} from 'react'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import buildQueries from 'utils/buildQueriesForGraphs'

const DashVisualization = (
  {
    axes,
    type,
    name,
    templates,
    timeRange,
    autoRefresh,
    queryConfigs,
    editQueryStatus,
  },
  {source: {links: {proxy}}}
) =>
  <div className="graph">
    <div className="graph-heading">
      <div className="graph-title">
        {name}
      </div>
    </div>
    <div className="graph-container">
      <RefreshingGraph
        axes={axes}
        type={type}
        queries={buildQueries(proxy, queryConfigs, timeRange)}
        templates={templates}
        autoRefresh={autoRefresh}
        editQueryStatus={editQueryStatus}
      />
    </div>
  </div>

const {arrayOf, func, number, shape, string} = PropTypes

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
}

DashVisualization.contextTypes = {
  source: PropTypes.shape({
    links: PropTypes.shape({
      proxy: PropTypes.string.isRequired,
    }).isRequired,
  }).isRequired,
}

export default DashVisualization
