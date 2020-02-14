// Libraries
import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'
import ReactGridLayout, {WidthProvider, Layout} from 'react-grid-layout'
import {get} from 'lodash'

// Components
const Grid = WidthProvider(ReactGridLayout)
import CellComponent from 'src/shared/components/cells/Cell'
import GradientBorder from 'src/shared/components/cells/GradientBorder'

// Utils
import {fastMap} from 'src/utils/fast'

// Constants
import {LAYOUT_MARGIN, DASHBOARD_LAYOUT_ROW_HEIGHT} from 'src/shared/constants'

// Types
import {AppState, Cell, TimeRange, RemoteDataState, View} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'
type ViewsByID = {[viewID: string]: View}

interface StateProps {
  views: ViewsByID
}

interface OwnProps {
  cells: Cell[]
  timeRange: TimeRange
  manualRefresh: number
  onPositionChange?: (cells: Cell[]) => void
}
type Props = StateProps & OwnProps & WithRouterProps

@ErrorHandling
class Cells extends Component<Props> {
  public render() {
    const {cells, timeRange, manualRefresh} = this.props

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
              timeRange={timeRange}
              manualRefresh={manualRefresh}
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
    const {views} = this.props
    return this.props.cells
      .filter(c => c.status === RemoteDataState.Done)
      .map(c => {
        const view = views[c.id]
        const cell = {
          ...c,
          x: c.x,
          y: c.y,
          h: c.h,
          w: c.w,
          i: c.id,
        }
        if (get(view, 'properties.type') === 'gauge') {
          cell.minW = 3
          cell.minH = 3
        }
        return cell
      })
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
}
const mstp = (state: AppState): StateProps => {
  return {views: state.resources.views.byID}
}
export default withRouter<OwnProps>(
  connect<StateProps, {}, OwnProps>(mstp)(Cells)
)
