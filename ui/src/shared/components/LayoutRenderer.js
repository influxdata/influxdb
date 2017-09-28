import React, {Component, PropTypes} from 'react'

import ReactGridLayout, {WidthProvider} from 'react-grid-layout'

import LayoutCell from 'shared/components/LayoutCell'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import AlertsApp from 'src/alerts/containers/AlertsApp'
import NewsFeed from 'src/status/components/NewsFeed'
import GettingStarted from 'src/status/components/GettingStarted'

import buildInfluxQLQuery, {buildCannedDashboardQuery} from 'utils/influxql'

import {
  // TODO: get these const values dynamically
  STATUS_PAGE_ROW_COUNT,
  PAGE_HEADER_HEIGHT,
  PAGE_CONTAINER_MARGIN,
  LAYOUT_MARGIN,
  DASHBOARD_LAYOUT_ROW_HEIGHT,
} from 'shared/constants'

import {RECENT_ALERTS_LIMIT} from 'src/status/constants'

const GridLayout = WidthProvider(ReactGridLayout)

class LayoutRenderer extends Component {
  constructor(props) {
    super(props)

    this.state = {
      rowHeight: this.calculateRowHeight(),
    }
  }

  buildQueries = (cell, source) => {
    const {timeRange, host} = this.props
    return cell.queries.map(query => {
      let queryText
      // Canned dashboards use an different a schema different from queryConfig.
      if (query.queryConfig) {
        const {queryConfig: {rawText, range}} = query
        const tR = range || {
          upper: ':upperDashboardTime:',
          lower: ':dashboardTime:',
        }
        queryText = rawText || buildInfluxQLQuery(tR, query.queryConfig)
      } else {
        queryText = buildCannedDashboardQuery(query, timeRange, host)
      }

      return Object.assign({}, query, {
        host: source.links.proxy,
        text: queryText,
      })
    })
  }

  generateWidgetCell = cell => {
    const {source, timeRange} = this.props

    switch (cell.type) {
      case 'alerts': {
        return (
          <AlertsApp
            source={source}
            timeRange={timeRange}
            isWidget={true}
            limit={RECENT_ALERTS_LIMIT}
          />
        )
      }
      case 'news': {
        return <NewsFeed source={source} />
      }
      case 'guide': {
        return <GettingStarted />
      }
    }

    return (
      <div className="graph-empty">
        <p data-test="data-explorer-no-results">Nothing to show</p>
      </div>
    )
  }

  // Generates cell contents based on cell type, i.e. graphs, news feeds, etc.
  generateVisualizations = () => {
    const {
      source,
      cells,
      onEditCell,
      onCancelEditCell,
      onDeleteCell,
      onSummonOverlayTechnologies,
      timeRange,
      autoRefresh,
      templates,
      synchronizer,
      isEditable,
      onZoom,
    } = this.props

    return cells.map(cell => {
      const {type, h, axes} = cell

      return (
        <div key={cell.i}>
          <LayoutCell
            onCancelEditCell={onCancelEditCell}
            isEditable={isEditable}
            onEditCell={onEditCell}
            onDeleteCell={onDeleteCell}
            onSummonOverlayTechnologies={onSummonOverlayTechnologies}
            cell={cell}
          >
            {cell.isWidget
              ? this.generateWidgetCell(cell)
              : <RefreshingGraph
                  timeRange={timeRange}
                  autoRefresh={autoRefresh}
                  templates={templates}
                  synchronizer={synchronizer}
                  type={type}
                  queries={this.buildQueries(cell, source)}
                  cellHeight={h}
                  axes={axes}
                  onZoom={onZoom}
                />}
          </LayoutCell>
        </div>
      )
    })
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

  // idea adopted from https://stackoverflow.com/questions/36862334/get-viewport-window-height-in-reactjs
  updateWindowDimensions = () => {
    this.setState({rowHeight: this.calculateRowHeight()})
  }

  render() {
    const {cells} = this.props
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
        {this.generateVisualizations()}
      </GridLayout>
    )
  }

  componentDidMount() {
    window.addEventListener('resize', this.updateWindowDimensions)
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.updateWindowDimensions)
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
