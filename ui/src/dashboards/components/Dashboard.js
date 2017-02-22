import React, {PropTypes} from 'react'
import classnames from 'classnames'

import LayoutRenderer from 'shared/components/LayoutRenderer'
import Visualizations from 'src/dashboards/components/VisualizationSelector'

const Dashboard = ({
  dashboard,
  isEditMode,
  inPresentationMode,
  onPositionChange,
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
        {Dashboard.renderDashboard(dashboard, timeRange, source, onPositionChange)}
      </div>
    </div>
  )
}

Dashboard.renderDashboard = (dashboard, timeRange, source, onPositionChange) => {
  const autoRefreshMs = 15000
  const cells = dashboard.cells.map((cell, i) => {
    i = `${i}`
    const dashboardCell = {...cell, i}
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
      onPositionChange={onPositionChange}
    />
  )
}

const {
  bool,
  func,
  shape,
  string,
} = PropTypes

Dashboard.propTypes = {
  dashboard: shape({}).isRequired,
  isEditMode: bool,
  inPresentationMode: bool,
  onPositionChange: func,
  source: shape({
    links: shape({
      proxy: string,
    }).isRequired,
  }).isRequired,
  timeRange: shape({}).isRequired,
}

export default Dashboard
