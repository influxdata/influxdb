import React, {PropTypes} from 'react'
import classnames from 'classnames'

import LayoutRenderer from 'shared/components/LayoutRenderer'
import Visualizations from 'src/dashboards/components/VisualizationSelector'

const Dashboard = ({
  dashboard,
  isEditMode,
  inPresentationMode,
  source,
  timeRange,
}) => {
  if (dashboard.id === 0) {
    return null
  }

  return (
    <div className={classnames({'page-contents': true, 'presentation-mode': inPresentationMode})}>
      <div className="container-fluid full-width dashboard-view">
        {isEditMode ? <Visualizations/> : null}
        {Dashboard.renderDashboard(dashboard, timeRange, source)}
      </div>
    </div>
  )
}

Dashboard.renderDashboard = (dashboard, timeRange, source) => {
  const autoRefreshMs = 15000
  const cellWidth = 4
  const cellHeight = 4
  const pageWidth = 12

  const cells = dashboard.cells.map((cell, i) => {
    const cellCount = i + 1;
    const cellConversion = {
      w: cellWidth,
      h: cellHeight,
      x: (cellCount * cellWidth % pageWidth),
      y: Math.floor(cellCount * cellWidth / pageWidth) * cellHeight,
      i: i.toString(),
    }

    const dashboardCell = {...cell, ...cellConversion}
    dashboardCell.queries.forEach((q) => {
      q.text = q.query;
      q.database = source.telegraf;
    });
    return dashboardCell;
  })

  return (
    <LayoutRenderer
      timeRange={timeRange}
      cells={cells}
      autoRefreshMs={autoRefreshMs}
      source={source.links.proxy}
    />
  )
}

const {
  bool,
  shape,
  string,
} = PropTypes

Dashboard.propTypes = {
  dashboard: shape({}).isRequired,
  isEditMode: bool,
  inPresentationMode: bool,
  source: shape({
    links: shape({
      proxy: string,
    }).isRequired,
  }).isRequired,
  timeRange: shape({}).isRequired,
}

export default Dashboard
