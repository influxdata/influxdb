import React, {PropTypes} from 'react'

import QueryBuilder from './QueryBuilder'
import QueryMakerTab from './QueryMakerTab'
import QueryTabList from 'src/dashboards/components/QueryTabList'
import buildInfluxQLQuery from 'utils/influxql'
import classnames from 'classnames'

const {arrayOf, bool, func, node, number, shape, string} = PropTypes

const QueryMaker = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        queries: string.isRequired,
      }).isRequired,
    }).isRequired,
    queries: arrayOf(shape({})).isRequired,
    timeRange: shape({
      upper: string,
      lower: string,
    }).isRequired,
    isInDataExplorer: bool,
    actions: shape({
      chooseNamespace: func.isRequired,
      chooseMeasurement: func.isRequired,
      chooseTag: func.isRequired,
      groupByTag: func.isRequired,
      addQuery: func.isRequired,
      toggleField: func.isRequired,
      groupByTime: func.isRequired,
      toggleTagAcceptance: func.isRequired,
      applyFuncsToField: func.isRequired,
      editRawTextAsync: func.isRequired,
    }).isRequired,
    height: string,
    top: string,
    setActiveQueryIndex: func.isRequired,
    onDeleteQuery: func.isRequired,
    activeQueryIndex: number,
    children: node,
    layout: string,
  },

  render() {
    const {
      height,
      top,
      layout,
      queries,
      timeRange,
      onAddQuery,
      onDeleteQuery,
      activeQueryIndex,
      setActiveQueryIndex,
    } = this.props

    return (
      <div
        className={classnames('query-maker', {
          'query-maker--panel': layout === 'panel',
        })}
        style={{height, top}}
      >
        <QueryTabList
          queries={queries}
          timeRange={timeRange}
          onAddQuery={onAddQuery}
          onDeleteQuery={onDeleteQuery}
          activeQueryIndex={activeQueryIndex}
          setActiveQueryIndex={setActiveQueryIndex}
        />
        {this.renderQueryBuilder()}
      </div>
    )
  },

  renderQueryBuilder() {
    const {
      timeRange,
      actions,
      source,
      layout,
      isInDataExplorer,
      activeQuery,
      onAddQuery,
    } = this.props

    if (!activeQuery) {
      return (
        <div className="query-maker--empty">
          <h5>This Graph has no Queries</h5>
          <br />
          <div
            className="btn btn-primary"
            role="button"
            onClick={onAddQuery}
            data-test="add-query-button"
          >
            Add a Query
          </div>
        </div>
      )
    }

    // NOTE
    // the layout prop is intended to toggle between a horizontal and vertical layout
    // the layout will be horizontal by default
    // vertical layout is known as "panel" layout as it will be used to build
    // a "cell editor panel" though that term might change
    // Currently, if set to "panel" the only noticeable difference is that the
    // DatabaseList becomes DatabaseDropdown (more space efficient in vertical layout)
    // and is outside the container with measurements/tags/fields
    //
    // TODO:
    // - perhaps switch to something like "isVertical" and accept boolean instead of string
    // - more css/markup work to make the alternate appearance look good

    return (
      <QueryBuilder
        source={source}
        timeRange={timeRange}
        query={activeQuery}
        actions={actions}
        onAddQuery={onAddQuery}
        layout={layout}
        isInDataExplorer={isInDataExplorer}
      />
    )
  },
})

QueryMaker.defaultProps = {
  layout: 'default',
}
export default QueryMaker
