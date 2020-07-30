// Libraries
import React, {Component} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import ReactGridLayout, {WidthProvider, Layout} from 'react-grid-layout'
import {get} from 'lodash'

// Components
const Grid = WidthProvider(ReactGridLayout)
import CellComponent from 'src/shared/components/cells/Cell'
import GradientBorder from 'src/shared/components/cells/GradientBorder'
import ScrollDetector from 'src/perf/components/ScrollDetector'

// Actions
import {updateCells} from 'src/cells/actions/thunks'

// Utils
import {fastMap} from 'src/utils/fast'
import {getCells} from 'src/cells/selectors'

// Constants
import {LAYOUT_MARGIN, DASHBOARD_LAYOUT_ROW_HEIGHT} from 'src/shared/constants'

// Types
import {AppState, Cell, RemoteDataState} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  manualRefresh: number
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps & ReduxProps

@ErrorHandling
class Cells extends Component<Props> {
  cellsRef = React.createRef()
  public render() {
    const {views, cells, manualRefresh} = this.props

    return (
      <>
        <ScrollDetector component="dashboard" />
        <Grid
          cols={12}
          layout={this.cells}
          rowHeight={DASHBOARD_LAYOUT_ROW_HEIGHT}
          useCSSTransforms={false}
          containerPadding={[0, 0]}
          margin={[LAYOUT_MARGIN, LAYOUT_MARGIN]}
          onLayoutChange={this.handleLayoutChange}
          draggableHandle=".cell--draggable"
          isDraggable
          isResizable
        >
          {fastMap(cells, cell => (
            <div
              key={cell.id}
              className="cell"
              data-testid={`cell ${views[cell.id]?.name}`}
            >
              <CellComponent cell={cell} manualRefresh={manualRefresh} />
              <GradientBorder />
            </div>
          ))}
        </Grid>
      </>
    )
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
          cell.minW = 5
          cell.minH = 2.5
          cell.maxW = 20
        }
        return cell
      })
  }

  private handleLayoutChange = grid => {
    const {cells} = this.props

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
      this.handlePositionChange(newCells)
    }
  }
  private handlePositionChange = (cells: Cell[]) => {
    const {dashboard, updateCells} = this.props
    updateCells(dashboard, cells)
  }
}
const mstp = (state: AppState) => {
  const dashboard = state.currentDashboard.id

  return {
    dashboard,
    cells: getCells(state, dashboard),
    views: state.resources.views.byID,
  }
}
const mdtp = {
  updateCells: updateCells,
}

const connector = connect(mstp, mdtp)

export default connector(Cells)
