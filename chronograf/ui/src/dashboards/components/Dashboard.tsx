import React, {PureComponent, MouseEvent} from 'react'
import classnames from 'classnames'

import Cells from 'src/shared/components/cells/Cells'
import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import DashboardEmpty from 'src/dashboards/components/DashboardEmpty'

import {Dashboard, Cell} from 'src/types/v2'
import {Template, TimeRange} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  dashboard: Dashboard
  timeRange: TimeRange
  templates: Template[]
  autoRefresh: number
  manualRefresh: number
  inPresentationMode: boolean
  inView: (cell: Cell) => boolean
  onDeleteCell: (cell: Cell) => void
  onCloneCell: (cell: Cell) => void
  onZoom: (range: TimeRange) => void
  onPositionChange: (cells: Cell[]) => void
  setScrollTop: (e: MouseEvent<JSX.Element>) => void
}

@ErrorHandling
class DashboardComponent extends PureComponent<Props> {
  public render() {
    const {
      onZoom,
      dashboard,
      timeRange,
      templates,
      autoRefresh,
      manualRefresh,
      onDeleteCell,
      onCloneCell,
      onPositionChange,
      inPresentationMode,
      setScrollTop,
    } = this.props

    return (
      <FancyScrollbar
        className={classnames('page-contents', {
          'presentation-mode': inPresentationMode,
        })}
        setScrollTop={setScrollTop}
      >
        <div className="dashboard container-fluid full-width">
          {dashboard.cells.length ? (
            <Cells
              onZoom={onZoom}
              templates={templates}
              timeRange={timeRange}
              autoRefresh={autoRefresh}
              manualRefresh={manualRefresh}
              cells={dashboard.cells}
              onCloneCell={onCloneCell}
              onDeleteCell={onDeleteCell}
              onPositionChange={onPositionChange}
            />
          ) : (
            <DashboardEmpty dashboard={dashboard} />
          )}
        </div>
      </FancyScrollbar>
    )
  }
}

export default DashboardComponent
