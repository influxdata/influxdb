import React, {PropTypes} from 'react'
import WidgetCell from 'shared/components/WidgetCell'
import LayoutCell from 'shared/components/LayoutCell'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import {buildQueriesForLayouts} from 'utils/influxql'

const Layout = ({
  host,
  cell,
  cell: {h, axes, type},
  source,
  onZoom,
  templates,
  timeRange,
  isEditable,
  onEditCell,
  autoRefresh,
  onDeleteCell,
  synchronizer,
  onCancelEditCell,
  onSummonOverlayTechnologies,
}) =>
  <LayoutCell
    onCancelEditCell={onCancelEditCell}
    isEditable={isEditable}
    onEditCell={onEditCell}
    onDeleteCell={onDeleteCell}
    onSummonOverlayTechnologies={onSummonOverlayTechnologies}
    cell={cell}
  >
    {cell.isWidget
      ? <WidgetCell cell={cell} timeRange={timeRange} source={source} />
      : <RefreshingGraph
          axes={axes}
          type={type}
          cellHeight={h}
          onZoom={onZoom}
          timeRange={timeRange}
          templates={templates}
          autoRefresh={autoRefresh}
          synchronizer={synchronizer}
          queries={buildQueriesForLayouts(cell, source, timeRange, host)}
        />}
  </LayoutCell>

const {arrayOf, bool, func, number, shape, string} = PropTypes

Layout.propTypes = {
  autoRefresh: number.isRequired,
  timeRange: shape({
    lower: string.isRequired,
  }),
  cell: shape({
    // isWidget cells will not have queries
    isWidget: bool,
    queries: arrayOf(
      shape({
        label: string,
        text: string,
        query: string,
      }).isRequired
    ),
    x: number.isRequired,
    y: number.isRequired,
    w: number.isRequired,
    h: number.isRequired,
    i: string.isRequired,
    name: string.isRequired,
    type: string.isRequired,
  }).isRequired,
  templates: arrayOf(shape()),
  host: string,
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
  onPositionChange: func,
  onEditCell: func,
  onDeleteCell: func,
  onSummonOverlayTechnologies: func,
  synchronizer: func,
  isStatusPage: bool,
  isEditable: bool,
  onCancelEditCell: func,
  onZoom: func,
}

export default Layout
