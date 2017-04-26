import React, {PropTypes} from 'react'
import classnames from 'classnames'

import omit from 'lodash/omit'

import LayoutRenderer from 'shared/components/LayoutRenderer'
import Dropdown from 'shared/components/Dropdown'

const Dashboard = ({
  source,
  timeRange,
  dashboard,
  onAddCell,
  onEditCell,
  autoRefresh,
  onRenameCell,
  onUpdateCell,
  onDeleteCell,
  onPositionChange,
  inPresentationMode,
  onOpenTemplateManager,
  onSummonOverlayTechnologies,
  onSelectTemplate,
}) => {
  if (dashboard.id === 0) {
    return null
  }

  const {templates} = dashboard

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
    <div
      className={classnames(
        'dashboard container-fluid full-width page-contents',
        {'presentation-mode': inPresentationMode}
      )}
    >
      <div className="template-control-bar">
        <div className="page-header__left">
          Template Variables
        </div>
        <div className="page-header__right">
          {templates.map(({id, values}) => {
            let selected
            const items = values.map(value => {
              if (value.selected) {
                selected = value.value
              }
              return {...value, text: value.value}
            })
            if (!selected && items.length) {
              selected = items[0].value
            }
            // TODO: change Dropdown to a MultiSelectDropdown, `selected` to
            // the full array, and [item] to all `selected` values when we update
            // this component to support multiple values
            return (
              <Dropdown
                key={id}
                items={items}
                selected={selected || 'Loading...'}
                onChoose={item =>
                  onSelectTemplate(id, [item].map(x => omit(x, 'text')))}
              />
            )
          })}
          <button
            className="btn btn-primary btn-sm"
            onClick={onOpenTemplateManager}
          >
            Manage
          </button>
        </div>
      </div>
      {cells.length
        ? <LayoutRenderer
            timeRange={timeRange}
            templates={templates}
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
        : <div className="dashboard__empty">
            <p>This Dashboard has no Graphs</p>
            <button className="btn btn-primary btn-m" onClick={onAddCell}>
              Add Graph
            </button>
          </div>}
    </div>
  )
}

const {arrayOf, bool, func, shape, string, number} = PropTypes

Dashboard.propTypes = {
  dashboard: shape({}).isRequired,
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
  onOpenTemplateManager: func.isRequired,
  onSelectTemplate: func.isRequired,
  templates: arrayOf(
    shape({
      type: string.isRequired,
      label: string.isRequired,
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
  ),
}

export default Dashboard
