import React, {Component, PropTypes} from 'react'
import ReactGridLayout, {WidthProvider} from 'react-grid-layout'
import Resizeable from 'react-component-resizable'

import _ from 'lodash'

import Authorized, {EDITOR_ROLE} from 'src/auth/Authorized'

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
      resizeCoords: null,
      annotationMode: null,
    }
  }

  handleStartAddAnnotation = () => {
    this.setState({annotationMode: 'adding'})
  }

  handleLayoutChange = layout => {
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

  handleCellResize = (__, oldCoords, resizeCoords) => {
    if (_.isEqual(oldCoords, resizeCoords)) {
      return
    }

    this.setState({resizeCoords})
  }

  render() {
    const {
      host,
      cells,
      source,
      sources,
      onZoom,
      templates,
      timeRange,
      isEditable,
      onEditCell,
      autoRefresh,
      manualRefresh,
      onDeleteCell,
      synchronizer,
      onCancelEditCell,
      onSummonOverlayTechnologies,
    } = this.props

    const {rowHeight, resizeCoords, annotationMode} = this.state
    const isDashboard = !!this.props.onPositionChange

    return (
      <Resizeable onResize={this.handleCellResize}>
        <Authorized
          requiredRole={EDITOR_ROLE}
          propsOverride={{
            isDraggable: false,
            isResizable: false,
            draggableHandle: null,
          }}
        >
          <GridLayout
            layout={cells}
            cols={12}
            rowHeight={rowHeight}
            margin={[LAYOUT_MARGIN, LAYOUT_MARGIN]}
            containerPadding={[0, 0]}
            useCSSTransforms={false}
            onResize={this.handleCellResize}
            onLayoutChange={this.handleLayoutChange}
            draggableHandle={'.dash-graph--draggable'}
            isDraggable={isDashboard}
            isResizable={isDashboard}
          >
            {cells.map(cell =>
              <div key={cell.i}>
                <Authorized
                  requiredRole={EDITOR_ROLE}
                  propsOverride={{
                    isEditable: false,
                  }}
                >
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
                    onEditCell={onEditCell}
                    autoRefresh={autoRefresh}
                    resizeCoords={resizeCoords}
                    onDeleteCell={onDeleteCell}
                    synchronizer={synchronizer}
                    manualRefresh={manualRefresh}
                    annotationMode={annotationMode}
                    onCancelEditCell={onCancelEditCell}
                    onStartAddAnnotation={this.handleStartAddAnnotation}
                    onSummonOverlayTechnologies={onSummonOverlayTechnologies}
                  />
                </Authorized>
              </div>
            )}
          </GridLayout>
        </Authorized>
      </Resizeable>
    )
  }
}

const {arrayOf, bool, func, number, shape, string} = PropTypes

LayoutRenderer.propTypes = {
  autoRefresh: number.isRequired,
  manualRefresh: number,
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
  sources: arrayOf(shape({})),
}

export default LayoutRenderer
