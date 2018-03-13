import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import RefreshingGraph from 'shared/components/RefreshingGraph'
import buildQueries from 'utils/buildQueriesForGraphs'
import VisualizationName from 'src/dashboards/components/VisualizationName'

import {stringifyColorValues} from 'src/dashboards/constants/gaugeColors'

const DashVisualization = (
  {
    axes,
    type,
    templates,
    timeRange,
    autoRefresh,
    gaugeColors,
    queryConfigs,
    editQueryStatus,
    resizerTopHeight,
    staticLegend,
    singleStatColors,
    tableOptions,
  },
  {source: {links: {proxy}}}
) => {
  const colors = type === 'gauge' ? gaugeColors : singleStatColors

  return (
    <div className="graph">
      <VisualizationName />
      <div className="graph-container">
        <RefreshingGraph
          colors={stringifyColorValues(colors)}
          axes={axes}
          type={type}
          tableOptions={tableOptions}
          queries={buildQueries(proxy, queryConfigs, timeRange)}
          templates={templates}
          autoRefresh={autoRefresh}
          editQueryStatus={editQueryStatus}
          resizerTopHeight={resizerTopHeight}
          staticLegend={staticLegend}
        />
      </div>
    </div>
  )
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

DashVisualization.propTypes = {
  type: string.isRequired,
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
  tableOptions: shape({}),
  resizerTopHeight: number,
  singleStatColors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: number.isRequired,
    }).isRequired
  ),
  gaugeColors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: number.isRequired,
    }).isRequired
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

const mapStateToProps = ({
  cellEditorOverlay: {
    singleStatColors,
    gaugeColors,
    cell: {type, axes, tableOptions},
  },
}) => ({
  gaugeColors,
  singleStatColors,
  type,
  axes,
  tableOptions,
})

export default connect(mapStateToProps, null)(DashVisualization)
