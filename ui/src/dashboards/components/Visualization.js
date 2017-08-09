import React, {PropTypes} from 'react'
import RefreshingGraph from 'shared/components/RefreshingGraph'
import buildInfluxQLQuery from 'utils/influxql'
import _ from 'lodash'

const {arrayOf, func, number, shape, string} = PropTypes

const DashVisualization = React.createClass({
  propTypes: {
    cellName: string,
    cellType: string,
    autoRefresh: number.isRequired,
    templates: arrayOf(shape()),
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    queryConfigs: arrayOf(shape({})).isRequired,
    height: string,
    heightPixels: number,
    editQueryStatus: func.isRequired,
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

  getDefaultProps() {
    return {
      cellName: '',
      cellType: '',
    }
  },

  render() {
    const {
      axes,
      height,
      cellType,
      cellName,
      templates,
      autoRefresh,
      heightPixels,
      editQueryStatus,
    } = this.props

    return (
      <div className="graph" style={{height}}>
        <div className="graph-heading">
          <div className="graph-title">
            {cellName}
          </div>
        </div>
        <div className="graph-container">
          <RefreshingGraph
            axes={axes}
            type={cellType}
            queries={::this.buildQueries()}
            templates={templates}
            cellHeight={heightPixels}
            autoRefresh={autoRefresh}
            editQueryStatus={editQueryStatus}
          />
        </div>
      </div>
    )
  },

  buildQueries() {
    const {source: {links: {proxy}}} = this.context
    const {queryConfigs, timeRange} = this.props

    const statements = queryConfigs.map(query => {
      const text =
        query.rawText || buildInfluxQLQuery(query.range || timeRange, query)
      return {text, id: query.id, queryConfig: query}
    })

    const queries = statements.filter(s => s.text !== null).map(s => {
      return {host: [proxy], text: s.text, id: s.id, queryConfig: s.queryConfig}
    })

    return queries
  },

  getQueryText(queryConfigs, index) {
    // rawText can be null
    return _.get(queryConfigs, [`${index}`, 'rawText'], '') || ''
  },
})

export default DashVisualization
