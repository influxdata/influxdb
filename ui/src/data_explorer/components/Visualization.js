import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import classNames from 'classnames'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import SingleStat from 'shared/components/SingleStat'
import MultiTable from './MultiTable'

const RefreshingLineGraph = AutoRefresh(LineGraph)
const RefreshingSingleStat = AutoRefresh(SingleStat)

const {
  arrayOf,
  number,
  shape,
  string,
} = PropTypes

const Visualization = React.createClass({
  propTypes: {
    cellType: string,
    autoRefresh: number.isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    queryConfigs: arrayOf(shape({})).isRequired,
    name: string,
    activeQueryIndex: number,
    height: string,
    heightPixels: number,
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
      isGraphInView: true,
    }
  },

  handleToggleView() {
    this.setState({isGraphInView: !this.state.isGraphInView})
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

  render() {
    const {queryConfigs, timeRange, height, heightPixels} = this.props
    const {source} = this.context
    const proxyLink = source.links.proxy

    const {isGraphInView} = this.state
    const statements = queryConfigs.map((query) => {
      const text = query.rawText || buildInfluxQLQuery(timeRange, query)
      return {text, id: query.id}
    })
    const queries = statements.filter((s) => s.text !== null).map((s) => {
      return {host: [proxyLink], text: s.text, id: s.id}
    })

    return (
      <div className={classNames("graph", {active: true})} style={{height}}>
        <div className="graph-heading">
          <div className="graph-title">
            {name || "Graph"}
          </div>
          <div className="graph-actions">
            <ul className="toggle toggle-sm">
              <li onClick={this.handleToggleView} className={classNames("toggle-btn ", {active: isGraphInView})}>Graph</li>
              <li onClick={this.handleToggleView} className={classNames("toggle-btn ", {active: !isGraphInView})}>Table</li>
            </ul>
          </div>
        </div>
        <div className={classNames({"graph-container": isGraphInView, "table-container": !isGraphInView})}>
          {isGraphInView ?
            this.renderGraph(queries) :
            <MultiTable queries={queries} height={heightPixels} />}
        </div>
      </div>
    )
  },
})

export default Visualization
