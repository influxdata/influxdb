// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import ReactGridLayout, {WidthProvider, Layout} from 'react-grid-layout'

// Components
const Grid = WidthProvider(ReactGridLayout)
import CellComponent from 'src/shared/components/cells/Cell'
import GradientBorder from 'src/shared/components/cells/GradientBorder'

// Utils
import {fastMap} from 'src/utils/fast'

// Constants
import {LAYOUT_MARGIN, DASHBOARD_LAYOUT_ROW_HEIGHT} from 'src/shared/constants'

// Types
import {Cell} from 'src/types'
import {TimeRange} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  cells: Cell[]
  timeRange: TimeRange
  manualRefresh: number
  onCloneCell?: (cell: Cell) => void
  onDeleteCell?: (cell: Cell) => void
  onPositionChange?: (cells: Cell[]) => void
  onEditView: (cellID: string) => void
  onEditNote: (id: string) => void
}

@ErrorHandling
class Cells extends Component<Props & WithRouterProps> {
  public render() {
    const {
      cells,
      onDeleteCell,
      onCloneCell,
      timeRange,
      manualRefresh,
      onEditNote,
    } = this.props

    return (
      <Grid
        cols={12}
        layout={this.cells}
        rowHeight={DASHBOARD_LAYOUT_ROW_HEIGHT}
        useCSSTransforms={false}
        containerPadding={[0, 0]}
        margin={[LAYOUT_MARGIN, LAYOUT_MARGIN]}
        onLayoutChange={this.handleLayoutChange}
        draggableHandle=".cell--draggable"
        isDraggable={this.isDashboard}
        isResizable={this.isDashboard}
      >
        {fastMap(cells, cell => (
          <div key={cell.id} className="cell">
            <CellComponent
              cell={cell}
              manualRefresh={manualRefresh}
              timeRange={timeRange}
              onCloneCell={onCloneCell}
              onDeleteCell={onDeleteCell}
              onEditCell={this.handleEditCell(cell)}
              onEditNote={onEditNote}
            />
            {this.cellBorder}
          </div>
        ))}
      </Grid>
    )
  }

  private get cellBorder(): JSX.Element {
    if (this.isDashboard) {
      return <GradientBorder />
    }
  }

  private get cells(): Layout[] {
    return this.props.cells.map(c => ({
      ...c,
      x: c.x,
      y: c.y,
      h: c.h,
      w: c.w,
      i: c.id,
    }))
  }

  private get isDashboard(): boolean {
    return this.props.location.pathname.includes('dashboard')
  }

  private handleLayoutChange = grid => {
    const {onPositionChange, cells} = this.props
    if (!onPositionChange) {
      return
    }

    let changed = false

    const newCells = cells.map(cell => {
      const l = grid.find(ly => ly.i === cell.id)

      if (
        cell.x !== l.x ||
        cell.y !== l.y ||
        cell.h !== l.h ||
        cell.w !== l.w
      ) {
        changed = true
      }

      const newCell = {
        x: l.x,
        y: l.y,
        h: l.h,
        w: l.w,
      }

      return {
        ...cell,
        ...newCell,
      }
    })

    if (changed) {
      this.props.onPositionChange(newCells)
    }
  }

  private handleEditCell = (cell: Cell) => {
    const {onEditView} = this.props

    return () => onEditView(cell.id)
  }
}

export default withRouter(Cells)
