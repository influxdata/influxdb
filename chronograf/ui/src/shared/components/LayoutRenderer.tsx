// Libraries
import React, {Component} from 'react'
import ReactGridLayout, {WidthProvider} from 'react-grid-layout'

// Components
import Layout from 'src/shared/components/Layout'
const GridLayout = WidthProvider(ReactGridLayout)

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
import {TimeRange, Cell, Template, Source} from 'src/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  source: Source
  sources: Source[]
  cells: Cell[]
  timeRange: TimeRange
  templates: Template[]
  host: string
  autoRefresh: number
  manualRefresh: number
  isStatusPage: boolean
  isEditable: boolean
  onZoom?: () => void
  onCloneCell?: () => void
  onDeleteCell?: () => void
  onSummonOverlayTechnologies?: () => void
  onPositionChange?: (cells: Cell[]) => void
}

interface State {
  rowHeight: number
}

@ErrorHandling
class LayoutRenderer extends Component<Props, State> {
  constructor(props) {
    super(props)

    this.state = {
      rowHeight: this.calculateRowHeight(),
    }
  }

  public render() {
    const {
      host,
      cells,
      source,
      sources,
      onZoom,
      templates,
      timeRange,
      isEditable,
      autoRefresh,
      manualRefresh,
      onDeleteCell,
      onCloneCell,
      onSummonOverlayTechnologies,
    } = this.props

    const {rowHeight} = this.state
    const isDashboard = !!this.props.onPositionChange

    return (
      <GridLayout
        layout={cells}
        cols={12}
        rowHeight={rowHeight}
        margin={[LAYOUT_MARGIN, LAYOUT_MARGIN]}
        containerPadding={[0, 0]}
        useCSSTransforms={false}
        onLayoutChange={this.handleLayoutChange}
        draggableHandle={'.dash-graph--draggable'}
        isDraggable={isDashboard}
        isResizable={isDashboard}
      >
        {fastMap(cells, cell => (
          <div key={cell.i}>
            <Layout
              key={cell.i}
              cell={cell}
              host={host}
              source={source}
              onZoom={onZoom}
              sources={sources}
              templates={templates}
              timeRange={timeRange}
              isEditable={isEditable}
              autoRefresh={autoRefresh}
              onDeleteCell={onDeleteCell}
              onCloneCell={onCloneCell}
              manualRefresh={manualRefresh}
              onSummonOverlayTechnologies={onSummonOverlayTechnologies}
            />
          </div>
        ))}
      </GridLayout>
    )
  }

  private handleLayoutChange = layout => {
    if (!this.props.onPositionChange) {
      return
    }

    let changed = false

    const newCells = this.props.cells.map(cell => {
      const l = layout.find(ly => ly.i === cell.i)

      if (
        cell.x !== l.x ||
        cell.y !== l.y ||
        cell.h !== l.h ||
        cell.w !== l.w
      ) {
        changed = true
      }

      const newLayout = {
        x: l.x,
        y: l.y,
        h: l.h,
        w: l.w,
      }

      return {
        ...cell,
        ...newLayout,
      }
    })

    if (changed) {
      this.props.onPositionChange(newCells)
    }
  }

  // ensures that Status Page height fits the window
  private calculateRowHeight = () => {
    const {isStatusPage} = this.props

    return isStatusPage
      ? (window.innerHeight -
          STATUS_PAGE_ROW_COUNT * LAYOUT_MARGIN -
          PAGE_HEADER_HEIGHT -
          PAGE_CONTAINER_MARGIN -
          PAGE_CONTAINER_MARGIN) /
          STATUS_PAGE_ROW_COUNT
      : DASHBOARD_LAYOUT_ROW_HEIGHT
  }
}

export default LayoutRenderer
