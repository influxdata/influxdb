import React, {PropTypes} from 'react'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
import NameableGraph from 'shared/components/NameableGraph'
import ReactGridLayout, {WidthProvider} from 'react-grid-layout'

import timeRanges from 'hson!../data/timeRanges.hson'
import buildInfluxQLQuery from 'utils/influxql'

const GridLayout = WidthProvider(ReactGridLayout)

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)

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
    templates: arrayOf(shape()).isRequired,
    host: string,
    source: string,
    onPositionChange: func,
    onEditCell: func,
    onRenameCell: func,
    onUpdateCell: func,
    onDeleteCell: func,
    onSummonOverlayTechnologies: func,
    shouldNotBeEditable: bool,
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

  renderRefreshingGraph(type, queries, cellHeight) {
    const {autoRefresh, templates} = this.props

    if (type === 'single-stat') {
      return (
        <RefreshingSingleStat
          queries={[queries[0]]}
          templates={templates}
          autoRefresh={autoRefresh}
          cellHeight={cellHeight}
        />
      )
    }

    const displayOptions = {
      stepPlot: type === 'line-stepplot',
      stackedGraph: type === 'line-stacked',
    }

    return (
      <RefreshingLineGraph
        queries={queries}
        templates={templates}
        autoRefresh={autoRefresh}
        showSingleStat={type === 'line-plus-single-stat'}
        displayOptions={displayOptions}
      />
    )
  },

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
    } = this.props

    return cells.map(cell => {
      const queries = cell.queries.map(query => {
        // TODO: Canned dashboards (and possibly Kubernetes dashboard) use an old query schema,
        // which does not have enough information for the new `buildInfluxQLQuery` function
        // to operate on. We will use `buildQueryForOldQuerySchema` until we conform
        // on a stable query representation.
        let queryText
        if (query.queryConfig) {
          const {queryConfig: {rawText, range}} = query
          const timeRange = range || {upper: null, lower: ':dashboardTime:'}
          queryText =
            rawText || buildInfluxQLQuery(timeRange, query.queryConfig)
        } else {
          queryText = this.buildQueryForOldQuerySchema(query)
        }

        return Object.assign({}, query, {
          host: source,
          text: queryText,
        })
      })

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
            {this.renderRefreshingGraph(cell.type, queries, cell.h)}
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
