import React, {PureComponent} from 'react'
import classnames from 'classnames'

import Cells from 'src/shared/components/cells/Cells'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import DashboardEmpty from 'src/dashboards/components/dashboard_empty/DashboardEmpty'

import {Dashboard, Cell} from 'src/types'
import {TimeRange} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  dashboard: Dashboard
  timeRange: TimeRange
  manualRefresh: number
  inPresentationMode: boolean
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onPositionChange: (cells: Cell[]) => void
  onEditView: (cellID: string) => void
  onAddCell: () => void
  onEditNote: (id: string) => void
}

@ErrorHandling
class DashboardComponent extends PureComponent<Props> {
  public render() {
    const {
      dashboard,
      timeRange,
      manualRefresh,
      onDeleteCell,
      onCloneCell,
      onEditView,
      onPositionChange,
      inPresentationMode,
      onAddCell,
      onEditNote,
    } = this.props

    return (
      <FancyScrollbar
        className={classnames('page-contents', {
          'presentation-mode': inPresentationMode,
        })}
      >
        <div className="dashboard container-fluid full-width">
          {dashboard.cells.length ? (
            <Cells
              timeRange={timeRange}
              manualRefresh={manualRefresh}
              cells={dashboard.cells}
              onCloneCell={onCloneCell}
              onDeleteCell={onDeleteCell}
              onPositionChange={onPositionChange}
              onEditView={onEditView}
              onEditNote={onEditNote}
            />
          ) : (
            <DashboardEmpty onAddCell={onAddCell} />
          )}
          {/* This element is used as a portal container for note tooltips in cell headers */}
          <div className="cell-header-note-tooltip-container" />
        </div>
      </FancyScrollbar>
    )
  }
}

export default DashboardComponent
