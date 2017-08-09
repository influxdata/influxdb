import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import VisHeader from 'src/data_explorer/components/VisHeader'
import VisView from 'src/data_explorer/components/VisView'
import {GRAPH} from 'shared/constants'
import _ from 'lodash'

const {arrayOf, bool, func, number, shape, string} = PropTypes

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
    resizerBottomHeight: number,
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
      resizerBottomHeight,
    } = this.props

    const {source: {links: {proxy}}} = this.context
    const statements = queryConfigs.map(query => {
      const text =
        query.rawText || buildInfluxQLQuery(query.range || timeRange, query)
      return {text, id: query.id, queryConfig: query}
    })

    const queries = statements.filter(s => s.text !== null).map(s => {
      return {host: [proxy], text: s.text, id: s.id, queryConfig: s.queryConfig}
    })

    return (
      <div className="graph" style={{height}}>
        <VisHeader
          views={views}
          view={GRAPH}
          onToggleView={this.handleToggleView}
          name={cellName}
        />
        <div className="graph-container">
          <VisView
            view={GRAPH}
            axes={axes}
            queries={queries}
            templates={templates}
            cellType={cellType}
            autoRefresh={autoRefresh}
            heightPixels={heightPixels}
            editQueryStatus={editQueryStatus}
            activeQueryIndex={activeQueryIndex}
            isInDataExplorer={isInDataExplorer}
            resizerBottomHeight={resizerBottomHeight}
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
