import React, {PropTypes} from 'react'
import classnames from 'classnames'

import LayoutRenderer from 'shared/components/LayoutRenderer'

const Dashboard = ({
  dashboard,
  isEditMode,
  inPresentationMode,
  onAddCell,
  onPositionChange,
  onEditCell,
  onRenameCell,
  onUpdateCell,
  onDeleteCell,
  onSummonOverlayTechnologies,
  source,
  autoRefresh,
  timeRange,
}) => {
  if (dashboard.id === 0) {
    return null
  }

  const cells = dashboard.cells.map((cell) => {
    const dashboardCell = {...cell}
    dashboardCell.queries = dashboardCell.queries.map(({label, query, queryConfig, db}) =>
      ({
        label,
        query,
        queryConfig,
        db,
        database: db,
        text: query,
      })
    )
    return dashboardCell
  })

  return (
    <div className={classnames({'page-contents': true, 'presentation-mode': inPresentationMode})}>
      {cells.length ?
        <div className={classnames('container-fluid full-width dashboard', {'dashboard-edit': isEditMode})}>
          <LayoutRenderer
            timeRange={timeRange}
            cells={cells}
            autoRefresh={autoRefresh}
            source={source.links.proxy}
            onPositionChange={onPositionChange}
            onEditCell={onEditCell}
            onRenameCell={onRenameCell}
            onUpdateCell={onUpdateCell}
            onDeleteCell={onDeleteCell}
            onSummonOverlayTechnologies={onSummonOverlayTechnologies}
          />
        </div> :
        <div className="dashboard__empty">
          <p>This Dashboard has no Graphs</p>
          <button
            className="btn btn-primary btn-m"
            onClick={onAddCell}
          >
            Add Graph
          </button>
        </div>
      }
    </div>
  )
}

const {
  bool,
  func,
  shape,
  string,
  number,
} = PropTypes

Dashboard.propTypes = {
  dashboard: shape({}).isRequired,
  isEditMode: bool,
  inPresentationMode: bool,
  onAddCell: func,
  onPositionChange: func,
  onEditCell: func,
  onRenameCell: func,
  onUpdateCell: func,
  onDeleteCell: func,
  onSummonOverlayTechnologies: func,
  source: shape({
    links: shape({
      proxy: string,
    }).isRequired,
  }).isRequired,
  autoRefresh: number.isRequired,
  timeRange: shape({}).isRequired,
}

export default Dashboard
