import React, {PropTypes} from 'react'

import ReactGridLayout, {WidthProvider} from 'react-grid-layout'

import NameableGraph from 'shared/components/NameableGraph'
import RefreshingGraph from 'shared/components/RefreshingGraph'

import timeRanges from 'hson!shared/data/timeRanges.hson'
import buildInfluxQLQuery from 'utils/influxql'

const GridLayout = WidthProvider(ReactGridLayout)

const {arrayOf, bool, func, number, shape, string} = PropTypes

export const LayoutRenderer = React.createClass({
  propTypes: {
    autoRefresh: number.isRequired,
    timeRange: shape({
      lower: string.isRequired,
    }),
    cells: arrayOf(
      shape({
        queries: arrayOf(
          shape({
            label: string,
            text: string,
            query: string,
          }).isRequired
        ).isRequired,
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
    source: string,
    onPositionChange: func,
    onEditCell: func,
    onRenameCell: func,
    onUpdateCell: func,
    onDeleteCell: func,
    onSummonOverlayTechnologies: func,
    shouldNotBeEditable: bool,
    synchronizer: func,
  },

  buildQueryForOldQuerySchema(q) {
    const {timeRange: {lower}, host} = this.props
    const {defaultGroupBy} = timeRanges.find(range => range.lower === lower)
    const {wheres, groupbys} = q

    let text = q.text

    text += ` where time > ${lower}`

    if (host) {
      text += ` and \"host\" = '${host}'`
    }

    if (wheres && wheres.length > 0) {
      text += ` and ${wheres.join(' and ')}`
    }

    if (groupbys) {
      if (groupbys.find(g => g.includes('time'))) {
        text += ` group by ${groupbys.join(',')}`
      } else if (groupbys.length > 0) {
        text += ` group by time(${defaultGroupBy}),${groupbys.join(',')}`
      } else {
        text += ` group by time(${defaultGroupBy})`
      }
    } else {
      text += ` group by time(${defaultGroupBy})`
    }

    return text
  },

  conformQueries(cell, source) {
    return cell.queries.map(query => {
      // TODO: Canned dashboards (and possibly Kubernetes dashboard) use an old query schema,
      // which does not have enough information for the new `buildInfluxQLQuery` function
      // to operate on. We will use `buildQueryForOldQuerySchema` until we conform
      // on a stable query representation.
      let queryText
      if (query.queryConfig) {
        const {queryConfig: {rawText, range}} = query
        const timeRange = range || {upper: null, lower: ':dashboardTime:'}
        queryText = rawText || buildInfluxQLQuery(timeRange, query.queryConfig)
      } else {
        queryText = this.buildQueryForOldQuerySchema(query)
      }

      return Object.assign({}, query, {
        host: source,
        text: queryText,
      })
    })
  },

  isGraph(cell) {
    // TODO: get this list from server
    const GRAPH_TYPES = [
      'line',
      'line-stepplot',
      'line-stacked',
      'single-stat',
      'line-plus-single-stat',
    ]
    return GRAPH_TYPES.includes(cell.type)
  },

  renderRefreshingComponent(type) {
    if (type === 'alerts') {
      return (
        <div className="graph-empty">
          <p>Coming soon: Alerts list</p>
        </div>
      )
    }
    if (type === 'news') {
      return (
        <div className="graph-empty">
          <p>Coming soon: Newsfeed</p>
        </div>
      )
    }
    if (type === 'guide') {
      return (
        <div className="graph-empty">
          <p>Coming soon: Markdown-based guide</p>
        </div>
      )
    }

    return (
      <div className="graph-empty">
        <p>No Results</p>
      </div>
    )
  },

  // Generates cell contents based on cell type, i.e. graphs, news feeds, etc.
  generateVisualizations() {
    const {
      source,
      cells,
      onEditCell,
      onRenameCell,
      onUpdateCell,
      onDeleteCell,
      onSummonOverlayTechnologies,
      shouldNotBeEditable,
      timeRange,
      autoRefresh,
      templates,
      synchronizer,
    } = this.props

    return cells.map(cell => {
      const {type, h} = cell
      const queries = this.conformQueries(cell, source)

      return (
        <div key={cell.i}>
          <NameableGraph
            onEditCell={onEditCell}
            onRenameCell={onRenameCell}
            onUpdateCell={onUpdateCell}
            onDeleteCell={onDeleteCell}
            onSummonOverlayTechnologies={onSummonOverlayTechnologies}
            shouldNotBeEditable={shouldNotBeEditable}
            cell={cell}
          >
            {this.isGraph(cell)
              ? <RefreshingGraph
                  timeRange={timeRange}
                  autoRefresh={autoRefresh}
                  templates={templates}
                  synchronizer={synchronizer}
                  type={type}
                  queries={queries}
                  cellHeight={h}
                />
              : this.renderRefreshingComponent(cell.type)}
          </NameableGraph>
        </div>
      )
    })
  },

  handleLayoutChange(layout) {
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
  },

  render() {
    const layoutMargin = 4
    const isDashboard = !!this.props.onPositionChange

    return (
      <GridLayout
        layout={this.props.cells}
        cols={12}
        rowHeight={83.5}
        margin={[layoutMargin, layoutMargin]}
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
  },

  triggerWindowResize() {
    // Hack to get dygraphs to fit properly during and after resize (dispatchEvent is a global method on window).
    const evt = document.createEvent('CustomEvent') // MUST be 'CustomEvent'
    evt.initCustomEvent('resize', false, false, null)
    dispatchEvent(evt)
  },
})

export default LayoutRenderer
