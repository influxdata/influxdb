import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import classnames from 'classnames'
import VisHeader from 'src/data_explorer/components/VisHeader'
import VisView from 'src/data_explorer/components/VisView'
import {GRAPH, TABLE} from 'shared/constants'
import _ from 'lodash'

const {arrayOf, bool, func, number, shape, string} = PropTypes
const META_QUERY_REGEX = /^show/i

const Visualization = React.createClass({
  propTypes: {
    cellName: string,
    cellType: string,
    autoRefresh: number.isRequired,
    templates: arrayOf(shape()),
    isInDataExplorer: bool,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    queryConfigs: arrayOf(shape({})).isRequired,
    activeQueryIndex: number,
    height: string,
    heightPixels: number,
    editQueryStatus: func.isRequired,
    views: arrayOf(string).isRequired,
    axes: shape({
      y: shape({
        bounds: arrayOf(string),
      }),
    }),
  },

  contextTypes: {
    source: PropTypes.shape({
      links: PropTypes.shape({
        proxy: PropTypes.string.isRequired,
      }).isRequired,
    }).isRequired,
  },

  getInitialState() {
    const {activeQueryIndex, queryConfigs} = this.props
    const activeQueryText = this.getQueryText(queryConfigs, activeQueryIndex)

    return activeQueryText.match(META_QUERY_REGEX)
      ? {view: TABLE}
      : {view: GRAPH}
  },

  getDefaultProps() {
    return {
      cellName: '',
    }
  },

  componentWillReceiveProps(nextProps) {
    const {activeQueryIndex, queryConfigs} = nextProps
    const nextQueryText = this.getQueryText(queryConfigs, activeQueryIndex)
    const queryText = this.getQueryText(
      this.props.queryConfigs,
      this.props.activeQueryIndex
    )

    if (queryText === nextQueryText) {
      return
    }

    if (nextQueryText.match(META_QUERY_REGEX)) {
      return this.setState({view: TABLE})
    }

    this.setState({view: GRAPH})
  },

  handleToggleView(view) {
    this.setState({view})
  },

  render() {
    const {
      axes,
      views,
      height,
      cellType,
      cellName,
      timeRange,
      templates,
      autoRefresh,
      heightPixels,
      queryConfigs,
      editQueryStatus,
      activeQueryIndex,
      isInDataExplorer,
    } = this.props
    const {source: {links: {proxy}}} = this.context
    const {view} = this.state

    const statements = queryConfigs.map(query => {
      const text = query.rawText || buildInfluxQLQuery(timeRange, query)
      return {text, id: query.id}
    })
    const queries = statements.filter(s => s.text !== null).map(s => {
      return {host: [proxy], text: s.text, id: s.id}
    })

    return (
      <div className="graph" style={{height}}>
        <VisHeader
          views={views}
          view={view}
          onToggleView={this.handleToggleView}
          name={cellName}
        />
        <div
          className={classnames({
            'graph-container': view === GRAPH,
            'table-container': view === TABLE,
          })}
        >
          <VisView
            view={view}
            axes={axes}
            queries={queries}
            templates={templates}
            cellType={cellType}
            autoRefresh={autoRefresh}
            heightPixels={heightPixels}
            editQueryStatus={editQueryStatus}
            activeQueryIndex={activeQueryIndex}
            isInDataExplorer={isInDataExplorer}
          />
        </div>
      </div>
    )
  },

  getQueryText(queryConfigs, index) {
    // rawText can be null
    return _.get(queryConfigs, [`${index}`, 'rawText'], '') || ''
  },
})

export default Visualization
