// Libraries
import React, {Component} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import ReactGridLayout, {WidthProvider, Layout} from 'react-grid-layout'

// Components
const Grid = WidthProvider(ReactGridLayout)
import CellComponent from 'src/shared/components/cells/Cell'

// Utils
import {fastMap} from 'src/utils/fast'

// Constants
import {
  LAYOUT_MARGIN,
  PAGE_HEADER_HEIGHT,
  PAGE_CONTAINER_MARGIN,
  STATUS_PAGE_ROW_COUNT,
  DASHBOARD_LAYOUT_ROW_HEIGHT,
} from 'src/shared/constants'

// Types
import {Cell} from 'src/types/v2'
import {Template, TimeRange} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  cells: Cell[]
  timeRange: TimeRange
  templates: Template[]
  autoRefresh: number
  manualRefresh: number
  onZoom: (range: TimeRange) => void
  onCloneCell?: (cell: Cell) => void
  onDeleteCell?: (cell: Cell) => void
  onPositionChange?: (cells: Cell[]) => void
}

interface State {
  rowHeight: number
}

@ErrorHandling
class Cells extends Component<Props & WithRouterProps, State> {
  constructor(props) {
    super(props)

    this.state = {
      rowHeight: this.calculateRowHeight(),
    }
  }

  public render() {
    const {
      cells,
      onZoom,
      onDeleteCell,
      onCloneCell,
      timeRange,
      templates,
      autoRefresh,
      manualRefresh,
    } = this.props
    const {rowHeight} = this.state

    return (
      <Grid
        cols={12}
        layout={this.cells}
        rowHeight={rowHeight}
        useCSSTransforms={false}
        containerPadding={[0, 0]}
        margin={[LAYOUT_MARGIN, LAYOUT_MARGIN]}
        onLayoutChange={this.handleLayoutChange}
        draggableHandle={'.dash-graph--draggable'}
        isDraggable={this.isDashboard}
        isResizable={this.isDashboard}
      >
        {fastMap(cells, cell => (
          <div key={cell.id}>
            <CellComponent
              cell={cell}
              onZoom={onZoom}
              isEditable={true}
              templates={templates}
              autoRefresh={autoRefresh}
              manualRefresh={manualRefresh}
              timeRange={timeRange}
              onCloneCell={onCloneCell}
              onDeleteCell={onDeleteCell}
            />
          </div>
        ))}
      </Grid>
    )
  }

  private get cells(): Layout[] {
    return this.props.cells.map(c => ({...c, i: c.id}))
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

  // ensures that Status Page height fits the window
  private calculateRowHeight = () => {
    const {location} = this.props

    return location.pathname.includes('status')
      ? (window.innerHeight -
          STATUS_PAGE_ROW_COUNT * LAYOUT_MARGIN -
          PAGE_HEADER_HEIGHT -
          PAGE_CONTAINER_MARGIN -
          PAGE_CONTAINER_MARGIN) /
          STATUS_PAGE_ROW_COUNT
      : DASHBOARD_LAYOUT_ROW_HEIGHT
  }
}

export default withRouter(Cells)
