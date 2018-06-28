import React from 'react'
import PropTypes from 'prop-types'
import buildInfluxQLQuery from 'utils/influxql'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'
import underlayCallback from 'src/kapacitor/helpers/ruleGraphUnderlay'

const RefreshingLineGraph = AutoRefresh(LineGraph)

import {LINE_COLORS_RULE_GRAPH} from 'src/shared/constants/graphColorPalettes'

const {shape, string, func} = PropTypes
const RuleGraph = ({
  query,
  source,
  timeRange: {lower},
  timeRange,
  rule,
  onChooseTimeRange,
}) => {
  const autoRefreshMs = 30000
  const queryText = buildInfluxQLQuery({lower}, query)
  const queries = [{host: source.links.proxy, text: queryText}]

  if (!queryText) {
    return (
      <div className="rule-builder--graph-empty">
        <p>
          Select a <strong>Time-Series</strong> to preview on a graph
        </p>
      </div>
    )
  }

  return (
    <div className="rule-builder--graph">
      <div className="rule-builder--graph-options">
        <p>Preview Data from</p>
        <TimeRangeDropdown
          onChooseTimeRange={onChooseTimeRange}
          selected={timeRange}
          preventCustomTimeRange={true}
        />
      </div>
      <RefreshingLineGraph
        source={source}
        queries={queries}
        isGraphFilled={false}
        ruleValues={rule.values}
        autoRefresh={autoRefreshMs}
        colors={LINE_COLORS_RULE_GRAPH}
        underlayCallback={underlayCallback(rule)}
      />
    </div>
  )
}

RuleGraph.propTypes = {
  source: shape({
    links: shape({
      proxy: string.isRequired,
    }).isRequired,
  }).isRequired,
  query: shape({}).isRequired,
  rule: shape({}).isRequired,
  timeRange: shape({}).isRequired,
  onChooseTimeRange: func.isRequired,
}

export default RuleGraph
