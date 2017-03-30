import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import classNames from 'classnames'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
import MultiTable from './MultiTable'
import VisHeader from 'src/data_explorer/components/VisHeader'

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)
const VIEWS = ['graph', 'table']

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
    return {
      view: 'graph',
    }
  },

  handleToggleView(view) {
    this.setState({view})
  },

  render() {
    const {queryConfigs, timeRange, height, heightPixels, onEditRawStatus} = this.props
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
      <div className={classNames("graph", {active: true})} style={{height}}>
        <VisHeader views={VIEWS} view={view} onToggleView={this.handleToggleView} name={name || 'Graph'}/>
        <div className={classNames({"graph-container": view === 'graph', "table-container": view === 'table'})}>
          {this.renderVisualization(view, queries, heightPixels, onEditRawStatus)}
        </div>
      </div>
    )
  },

  renderVisualization(view, queries, heightPixels, onEditRawStatus) {
    switch (view) {
      case 'graph':
        return this.renderGraph(queries)
      case 'table':
        return <MultiTable queries={queries} height={heightPixels} onEditRawStatus={onEditRawStatus} />
      default:
        this.renderGraph(queries)
    }
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
