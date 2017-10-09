import React, {PropTypes} from 'react'
import buildInfluxQLQuery from 'utils/influxql'
import AutoRefresh from 'shared/components/AutoRefresh'
import LineGraph from 'shared/components/LineGraph'
import TimeRangeDropdown from 'shared/components/TimeRangeDropdown'

const RefreshingLineGraph = AutoRefresh(LineGraph)

const {shape, string, func} = PropTypes

export const RuleGraph = React.createClass({
  propTypes: {
    source: shape({
      links: shape({
        proxy: string.isRequired,
      }).isRequired,
    }).isRequired,
    query: shape({}).isRequired,
    rule: shape({}).isRequired,
    timeRange: shape({}).isRequired,
    onChooseTimeRange: func.isRequired,
  },

  render() {
    const {
      query,
      source,
      timeRange: {lower},
      timeRange,
      rule,
      onChooseTimeRange,
    } = this.props
    const autoRefreshMs = 30000
    const queryText = buildInfluxQLQuery({lower}, query)
    const queries = [{host: source.links.proxy, text: queryText}]
    const kapacitorLineColors = ['#4ED8A0']

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
          queries={queries}
          autoRefresh={autoRefreshMs}
          underlayCallback={this.createUnderlayCallback()}
          isGraphFilled={false}
          overrideLineColors={kapacitorLineColors}
          ruleValues={rule.values}
        />
      </div>
    )
  },

  _getFillColor(operator) {
    const backgroundColor = 'rgba(41, 41, 51, 1)'
    const highlightColor = 'rgba(78, 216, 160, 0.3)'

    if (operator === 'outside range') {
      return backgroundColor
    }

    if (operator === 'not equal to') {
      return backgroundColor
    }

    return highlightColor
  },

  createUnderlayCallback() {
    const {rule} = this.props
    return (canvas, area, dygraph) => {
      if (rule.trigger !== 'threshold' || rule.values.value === '') {
        return
      }

      const theOnePercent = 0.01
      let highlightStart = 0
      let highlightEnd = 0

      switch (rule.values.operator) {
        case 'equal to or greater':
        case 'greater than': {
          highlightStart = rule.values.value
          highlightEnd = dygraph.yAxisRange()[1]
          break
        }

        case 'equal to or less than':
        case 'less than': {
          highlightStart = dygraph.yAxisRange()[0]
          highlightEnd = rule.values.value
          break
        }

        case 'equal to': {
          const width =
            theOnePercent * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0])
          highlightStart = +rule.values.value - width
          highlightEnd = +rule.values.value + width
          break
        }

        case 'not equal to': {
          const width =
            theOnePercent * (dygraph.yAxisRange()[1] - dygraph.yAxisRange()[0])
          highlightStart = +rule.values.value - width
          highlightEnd = +rule.values.value + width

          canvas.fillStyle = 'rgba(78, 216, 160, 0.3)'
          canvas.fillRect(area.x, area.y, area.w, area.h)
          break
        }

        case 'outside range': {
          const {rangeValue, value} = rule.values
          highlightStart = Math.min(+value, +rangeValue)
          highlightEnd = Math.max(+value, +rangeValue)

          canvas.fillStyle = 'rgba(78, 216, 160, 0.3)'
          canvas.fillRect(area.x, area.y, area.w, area.h)
          break
        }
        case 'inside range': {
          const {rangeValue, value} = rule.values
          highlightStart = Math.min(+value, +rangeValue)
          highlightEnd = Math.max(+value, +rangeValue)
          break
        }
      }

      const bottom = dygraph.toDomYCoord(highlightStart)
      const top = dygraph.toDomYCoord(highlightEnd)

      const fillColor = this._getFillColor(rule.values.operator)
      canvas.fillStyle = fillColor
      canvas.fillRect(area.x, top, area.w, bottom - top)
    }
  },
})

export default RuleGraph
