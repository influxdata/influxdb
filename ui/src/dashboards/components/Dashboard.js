import React, {PropTypes} from 'react'
import classnames from 'classnames'

import TemplateControlBar from 'src/dashboards/components/TemplateControlBar'
import LayoutRenderer from 'shared/components/LayoutRenderer'
import FancyScrollbar from 'shared/components/FancyScrollbar'

const Dashboard = ({
  source,
  onZoom,
  dashboard,
  onAddCell,
  onEditCell,
  timeRange,
  autoRefresh,
  onRenameCell,
  onUpdateCell,
  onDeleteCell,
  synchronizer,
  editQueryStatus,
  onSelectTemplate,
  onPositionChange,
  onCancelEditCell,
  inPresentationMode,
  onOpenTemplateManager,
  showTemplateControlBar,
  templatesIncludingDashTime,
  onSummonOverlayTechnologies,
}) => {
  const cells = dashboard.cells.map(cell => {
    const dashboardCell = {...cell}
    dashboardCell.queries = dashboardCell.queries.map(
      ({label, query, queryConfig, db}) => ({
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
    <FancyScrollbar
      className={classnames('page-contents', {
        'presentation-mode': inPresentationMode,
      })}
    >
      <div className="dashboard container-fluid full-width">
        {inPresentationMode
          ? null
          : <TemplateControlBar
              templates={dashboard.templates}
              onSelectTemplate={onSelectTemplate}
              onOpenTemplateManager={onOpenTemplateManager}
              isOpen={showTemplateControlBar}
            />}
        {cells.length
          ? <LayoutRenderer
              onCancelEditCell={onCancelEditCell}
              templates={templatesIncludingDashTime}
              isEditable={true}
              cells={cells}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              source={source}
              onPositionChange={onPositionChange}
              onEditCell={onEditCell}
              onRenameCell={onRenameCell}
              onUpdateCell={onUpdateCell}
              onDeleteCell={onDeleteCell}
              onSummonOverlayTechnologies={onSummonOverlayTechnologies}
              synchronizer={synchronizer}
              onZoom={onZoom}
              editQueryStatus={editQueryStatus}
            />
          : <div className="dashboard__empty">
              <p>This Dashboard has no Cells</p>
              <button className="btn btn-primary btn-m" onClick={onAddCell}>
                <span className="icon plus" />Add a Cell
              </button>
            </div>}
      </div>
    </FancyScrollbar>
  )
}

const {arrayOf, bool, func, shape, string, number} = PropTypes

Dashboard.propTypes = {
  dashboard: shape({
    templates: arrayOf(
      shape({
        type: string.isRequired,
        tempVar: string.isRequired,
        query: shape({
          db: string,
          rp: string,
          influxql: string,
        }),
        values: arrayOf(
          shape({
            type: string.isRequired,
            value: string.isRequired,
            selected: bool,
          })
        ).isRequired,
      })
    ).isRequired,
  }),
  templatesIncludingDashTime: arrayOf(shape()).isRequired,
  inPresentationMode: bool,
  onAddCell: func,
  onPositionChange: func,
  onEditCell: func,
  onRenameCell: func,
  onUpdateCell: func,
  onDeleteCell: func,
  editQueryStatus: func,
  onSummonOverlayTechnologies: func,
  synchronizer: func,
  source: shape({
    links: shape({
      proxy: string,
    }).isRequired,
  }).isRequired,
  autoRefresh: number.isRequired,
  timeRange: shape({}).isRequired,
  onOpenTemplateManager: func.isRequired,
  onSelectTemplate: func.isRequired,
  showTemplateControlBar: bool,
  onCancelEditCell: func,
  onZoom: func,
}

export default Dashboard
