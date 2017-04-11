import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import classNames from 'classnames'
import VisHeader from 'src/data_explorer/components/VisHeader'
import View from 'src/data_explorer/components/View'

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
    fetchTimeSeries: func.isRequired,
    editQueryStatus: func,
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
    const {
      height,
      cellType,
      timeRange,
      autoRefresh,
      heightPixels,
      queryConfigs,
      editQueryStatus,
      fetchTimeSeries,
      activeQueryIndex,
    } = this.props
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
          <View
            view={view}
            queries={queries}
            cellType={cellType}
            autoRefresh={autoRefresh}
            heightPixels={heightPixels}
            editQueryStatus={editQueryStatus}
            fetchTimeSeries={fetchTimeSeries}
            activeQueryIndex={activeQueryIndex}
          />
        </div>
      </div>
    )
  },
})

export default Visualization
