import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import classNames from 'classnames'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
import Table from './Table'
import VisHeader from 'src/data_explorer/components/VisHeader'

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)

const GRAPH = 'graph'
const TABLE = 'table'
const VIEWS = [GRAPH, TABLE]

const {
  func,
  arrayOf,
  number,
  shape,
  string,
} = PropTypes

const Visualization = React.createClass({
  propTypes: {
    cellName: string,
    cellType: string,
    autoRefresh: number.isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    queryConfigs: arrayOf(shape({})).isRequired,
    activeQueryIndex: number,
    height: string,
    heightPixels: number,
    onEditRawStatus: func.isRequired,
  },

  contextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    const {queryConfigs, activeQueryIndex} = this.props
    if (!queryConfigs.length || activeQueryIndex === null) {
      return {
        view: GRAPH,
      }
    }

    return {
      view: typeof queryConfigs[activeQueryIndex].rawText === 'string' ? TABLE : GRAPH,
    }
  },

  componentWillReceiveProps(nextProps) {
    const {queryConfigs, activeQueryIndex} = nextProps
    if (!queryConfigs.length || activeQueryIndex === null || activeQueryIndex === this.props.activeQueryIndex) {
      return
    }

    const activeQuery = queryConfigs[activeQueryIndex]
    if (activeQuery && typeof activeQuery.rawText === 'string') {
      return this.setState({view: TABLE})
    }
  },

  handleToggleView(view) {
    this.setState({view})
  },

  render() {
    const {queryConfigs, timeRange, height, heightPixels, onEditRawStatus, activeQueryIndex} = this.props
    const {source} = this.context
    const proxyLink = source.links.proxy
    const {view} = this.state

    const statements = queryConfigs.map((query) => {
      const text = query.rawText || buildInfluxQLQuery(timeRange, query)
      return {text, id: query.id}
    })
    const queries = statements.filter((s) => s.text !== null).map((s) => {
      return {host: [proxyLink], text: s.text, id: s.id}
    })

    return (
      <div className="graph" style={{height}}>
        <VisHeader views={VIEWS} view={view} onToggleView={this.handleToggleView} name={name || 'Graph'}/>
        <div className={classNames({"graph-container": view === GRAPH, "table-container": view === TABLE})}>
          {this.renderVisualization(view, queries, heightPixels, onEditRawStatus, activeQueryIndex)}
        </div>
      </div>
    )
  },

  renderVisualization(view, queries, heightPixels, onEditRawStatus, activeQueryIndex) {
    const activeQuery = queries[activeQueryIndex]
    const defaultQuery = queries[0]

    switch (view) {
      case TABLE:
        return this.renderTable(activeQuery || defaultQuery, heightPixels, onEditRawStatus)
      case GRAPH:
      default:
        this.renderGraph(queries)
    }
  },

  renderTable(query, heightPixels, onEditRawStatus) {
    if (!query) {
      return <div className="generic-empty-state">Enter your query below</div>
    }

    return <Table query={query} height={heightPixels} onEditRawStatus={onEditRawStatus} />
  },

  renderGraph(queries) {
    const {cellType, autoRefresh, activeQueryIndex} = this.props
    const isInDataExplorer = true

    if (cellType === 'single-stat') {
      return <RefreshingSingleStat queries={[queries[0]]} autoRefresh={autoRefresh} />
    }

    const displayOptions = {
      stepPlot: cellType === 'line-stepplot',
      stackedGraph: cellType === 'line-stacked',
    }
    return (
      <RefreshingLineGraph
        queries={queries}
        autoRefresh={autoRefresh}
        activeQueryIndex={activeQueryIndex}
        isInDataExplorer={isInDataExplorer}
        showSingleStat={cellType === "line-plus-single-stat"}
        displayOptions={displayOptions}
      />
    )
  },
})

export default Visualization
