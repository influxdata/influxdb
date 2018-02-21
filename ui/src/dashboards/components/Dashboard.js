import React, {PropTypes} from 'react'
import classnames from 'classnames'

import TemplateControlBar from 'src/dashboards/components/TemplateControlBar'
import LayoutRenderer from 'shared/components/LayoutRenderer'
import FancyScrollbar from 'shared/components/FancyScrollbar'

const Dashboard = ({
  source,
  sources,
  onZoom,
  dashboard,
  onAddCell,
  timeRange,
  autoRefresh,
  manualRefresh,
  onDeleteCell,
  synchronizer,
  onPositionChange,
  inPresentationMode,
  onOpenTemplateManager,
  templatesIncludingDashTime,
  onSummonOverlayTechnologies,
  onSelectTemplate,
  showTemplateControlBar,
  getScrollTop,
  inView,
}) => {
  const cells = dashboard.cells.map(cell => {
    const dashboardCell = {
      ...cell,
      preventLoad: !inView(cell),
    }
    dashboardCell.queries = dashboardCell.queries.map(q => ({
      ...q,
      database: q.db,
      text: q.query,
    }))
    return dashboardCell
  })

  return (
    <FancyScrollbar
      className={classnames('page-contents', {
        'presentation-mode': inPresentationMode,
      })}
      getScrollTop={getScrollTop}
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
              cells={cells}
              onZoom={onZoom}
              source={source}
              sources={sources}
              isEditable={true}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              manualRefresh={manualRefresh}
              synchronizer={synchronizer}
              onDeleteCell={onDeleteCell}
              onPositionChange={onPositionChange}
              templates={templatesIncludingDashTime}
              onSummonOverlayTechnologies={onSummonOverlayTechnologies}
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
  onDeleteCell: func,
  onSummonOverlayTechnologies: func,
  synchronizer: func,
  source: shape({
    links: shape({
      proxy: string,
    }).isRequired,
  }).isRequired,
  sources: arrayOf(shape({})).isRequired,
  autoRefresh: number.isRequired,
  manualRefresh: number,
  timeRange: shape({}).isRequired,
  onOpenTemplateManager: func.isRequired,
  onSelectTemplate: func.isRequired,
  showTemplateControlBar: bool,
  onZoom: func,
  getScrollTop: func,
  inView: func,
}

export default Dashboard
