import React from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'

import RefreshingGraph from 'src/shared/components/RefreshingGraph'
import buildQueries from 'utils/buildQueriesForGraphs'
import VisualizationName from 'src/dashboards/components/VisualizationName'

import {stringifyColorValues} from 'src/shared/constants/colorOperations'

const DashVisualization = (
  {
    axes,
    type,
    templates,
    timeRange,
    lineColors,
    autoRefresh,
    gaugeColors,
    queryConfigs,
    editQueryStatus,
    resizerTopHeight,
    staticLegend,
    thresholdsListColors,
    tableOptions,
    setDataLabels,
  },
  {source: {links: {proxy}}}
) => {
  let colors = []
  switch (type) {
    case 'gauge': {
      colors = stringifyColorValues(gaugeColors)
      break
    }
    case 'single-stat':
    case 'line-plus-single-stat':
    case 'table': {
      colors = stringifyColorValues(thresholdsListColors)
      break
    }
    case 'bar':
    case 'line':
    case 'line-stacked':
    case 'line-stepplot': {
      colors = stringifyColorValues(lineColors)
    }
  }

  return (
    <div className="graph">
      <VisualizationName />
      <div className="graph-container">
        <RefreshingGraph
          colors={colors}
          axes={axes}
          type={type}
          tableOptions={tableOptions}
          queries={buildQueries(proxy, queryConfigs, timeRange)}
          templates={templates}
          autoRefresh={autoRefresh}
          editQueryStatus={editQueryStatus}
          resizerTopHeight={resizerTopHeight}
          staticLegend={staticLegend}
          setDataLabels={setDataLabels}
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
  thresholdsListColors: arrayOf(shape({}).isRequired),
  gaugeColors: arrayOf(shape({}).isRequired),
  lineColors: arrayOf(shape({}).isRequired),
  staticLegend: bool,
  setDataLabels: func,
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
    thresholdsListColors,
    gaugeColors,
    lineColors,
    cell: {type, axes, tableOptions},
  },
}) => ({
  gaugeColors,
  thresholdsListColors,
  lineColors,
  type,
  axes,
  tableOptions,
})

export default connect(mapStateToProps, null)(DashVisualization)
