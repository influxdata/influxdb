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
      availableHeight: 0,
      scrollTop: 0,
    }
  }

  handleLayoutChange = layout => {
    console.log('here')
    if (!this.props.onPositionChange) {
      return
    }
    const newCells = this.props.cells.map(cell => {
      const l = layout.find(ly => ly.i === cell.i)
      if (l) {
        const newLayout = {x: l.x, y: l.y, h: l.h, w: l.w}
        return {...cell, ...newLayout}
      }
      return cell
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

  handleScroll = event => {
    this.setState({
      scrollTop: event.target.scrollTop,
    })
  }

  handleWindowResize = () => {
    this.setState({availableHeight: window.innerHeight})
  }

  componentDidMount() {
    this.handleWindowResize()
    window.addEventListener('resize', this.handleWindowResize, true)
    window.addEventListener('scroll', this.handleScroll, true)
  }

  componentWillUnMount() {
    window.removeEventListener('resize', this.handleWindowResize, true)
    window.removeEventListener('scroll', this.handleScroll, true)
  }

  lazyLoadFilter = cell => {
    const {isStatusPage} = this.props
    const {availableHeight, scrollTop} = this.state

    if (isStatusPage) {
      return true
    }

    return (
      cell.y * DASHBOARD_LAYOUT_ROW_HEIGHT < availableHeight + scrollTop &&
      (cell.y + cell.h) * DASHBOARD_LAYOUT_ROW_HEIGHT > scrollTop
    )
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

    const {rowHeight, resizeCoords, availableHeight, scrollTop} = this.state
    const isDashboard = !!this.props.onPositionChange
    const filteredCells = cells.filter(this.lazyLoadFilter)

    const mappedCells = cells.map(cell => {
      return {...cell, dontload: !this.lazyLoadFilter(cell)}
    })

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
            {mappedCells.map(cell =>
              <div key={cell.i}>
                {cell.y * DASHBOARD_LAYOUT_ROW_HEIGHT <
                  availableHeight + scrollTop &&
                (cell.y + cell.h) * DASHBOARD_LAYOUT_ROW_HEIGHT > scrollTop
                  ? <Authorized
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
                        resizeCoords={resizeCoords}
                        autoRefresh={autoRefresh}
                        manualRefresh={manualRefresh}
                        onDeleteCell={onDeleteCell}
                        synchronizer={synchronizer}
                        onCancelEditCell={onCancelEditCell}
                        onSummonOverlayTechnologies={
                          onSummonOverlayTechnologies
                        }
                      />
                    </Authorized>
                  : null}
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
