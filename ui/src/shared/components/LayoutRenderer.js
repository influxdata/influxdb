import React, {Component, PropTypes} from 'react'

import ReactGridLayout, {WidthProvider} from 'react-grid-layout'
import Layout from 'src/shared/components/Layout'

import {
  // TODO: get these const values dynamically
  STATUS_PAGE_ROW_COUNT,
  PAGE_HEADER_HEIGHT,
  PAGE_CONTAINER_MARGIN,
  LAYOUT_MARGIN,
  DASHBOARD_LAYOUT_ROW_HEIGHT,
} from 'shared/constants'

const GridLayout = WidthProvider(ReactGridLayout)

class LayoutRenderer extends Component {
  constructor(props) {
    super(props)

    this.state = {
      rowHeight: this.calculateRowHeight(),
    }
  }

  componentDidMount() {
    window.addEventListener('resize', this.updateWindowDimensions)
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.updateWindowDimensions)
  }

  // idea adopted from https://stackoverflow.com/questions/36862334/get-viewport-window-height-in-reactjs
  updateWindowDimensions = () => {
    this.setState({rowHeight: this.calculateRowHeight()})
  }

  handleLayoutChange = layout => {
    this.triggerWindowResize()

    if (!this.props.onPositionChange) {
      return
    }

    const newCells = this.props.cells.map(cell => {
      const l = layout.find(ly => ly.i === cell.i)
      const newLayout = {x: l.x, y: l.y, h: l.h, w: l.w}
      return {...cell, ...newLayout}
    })

    this.props.onPositionChange(newCells)
  }

  triggerWindowResize = () => {
    // Hack to get dygraphs to fit properly during and after resize (dispatchEvent is a global method on window).
    const evt = document.createEvent('CustomEvent') // MUST be 'CustomEvent'
    evt.initCustomEvent('resize', false, false, null)
    dispatchEvent(evt)
  }

  // ensures that Status Page height fits the window
  calculateRowHeight = () => {
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

  render() {
    const {
      host,
      cells,
      source,
      onZoom,
      templates,
      timeRange,
      isEditable,
      onEditCell,
      autoRefresh,
      onDeleteCell,
      synchronizer,
      onCancelEditCell,
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
        onResize={this.triggerWindowResize}
        onLayoutChange={this.handleLayoutChange}
        draggableHandle={'.dash-graph--name'}
        isDraggable={isDashboard}
        isResizable={isDashboard}
      >
        {cells.map(cell =>
          <div key={cell.i}>
            <Layout
              key={cell.i}
              cell={cell}
              host={host}
              source={source}
              onZoom={onZoom}
              templates={templates}
              timeRange={timeRange}
              isEditable={isEditable}
              onEditCell={onEditCell}
              autoRefresh={autoRefresh}
              onDeleteCell={onDeleteCell}
              synchronizer={synchronizer}
              onCancelEditCell={onCancelEditCell}
              onSummonOverlayTechnologies={onSummonOverlayTechnologies}
            />
          </div>
        )}
      </GridLayout>
    )
  }
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

LayoutRenderer.propTypes = {
  autoRefresh: number.isRequired,
  timeRange: shape({
    lower: string.isRequired,
  }),
  cells: arrayOf(
    shape({
      // isWidget cells will not have queries
      isWidget: bool,
      queries: arrayOf(
        shape({
          label: string,
          text: string,
          query: string,
        }).isRequired
      ),
      x: number.isRequired,
      y: number.isRequired,
      w: number.isRequired,
      h: number.isRequired,
      i: string.isRequired,
      name: string.isRequired,
      type: string.isRequired,
    }).isRequired
  ),
  templates: arrayOf(shape()),
  host: string,
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
  onPositionChange: func,
  onEditCell: func,
  onDeleteCell: func,
  onSummonOverlayTechnologies: func,
  synchronizer: func,
  isStatusPage: bool,
  isEditable: bool,
  onCancelEditCell: func,
  onZoom: func,
}

export default LayoutRenderer
