import React, {PureComponent} from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'
import lastValues from 'shared/parsing/lastValues'

import {SMALL_CELL_HEIGHT} from 'shared/graphs/helpers'
import {DYGRAPH_CONTAINER_V_MARGIN} from 'shared/constants'
import {generateThresholdsListHexs} from 'shared/constants/colorOperations'

class SingleStat extends PureComponent {
  render() {
    const {
      data,
      cellHeight,
      isFetchingInitially,
      colors,
      prefix,
      suffix,
      lineGraph,
      staticLegendHeight,
    } = this.props

    // If data for this graph is being fetched for the first time, show a graph-wide spinner.
    if (isFetchingInitially) {
      return (
        <div className="graph-empty">
          <h3 className="graph-spinner" />
        </div>
      )
    }

    const lastValue = lastValues(data)[1]
    const precision = 100.0
    const roundedValue = Math.round(+lastValue * precision) / precision

    const {bgColor, textColor} = generateThresholdsListHexs(
      colors,
      lastValue,
      lineGraph
    )

    const backgroundColor = bgColor
    const color = textColor
    const height = `calc(100% - ${staticLegendHeight +
      DYGRAPH_CONTAINER_V_MARGIN * 2}px)`

    const singleStatStyles = staticLegendHeight
      ? {
          backgroundColor,
          color,
          height,
        }
      : {
          backgroundColor,
          color,
        }

    return (
      <div className="single-stat" style={singleStatStyles}>
        <span
          className={classnames('single-stat--value', {
            'single-stat--small': cellHeight === SMALL_CELL_HEIGHT,
          })}
        >
          {prefix}
          {roundedValue}
          {suffix}
          {lineGraph && <div className="single-stat--shadow" />}
        </span>
      </div>
    )
  }
}

const {arrayOf, bool, number, shape, string} = PropTypes

SingleStat.propTypes = {
  data: arrayOf(shape()).isRequired,
  isFetchingInitially: bool,
  cellHeight: number,
  colors: arrayOf(
    shape({
      type: string.isRequired,
      hex: string.isRequired,
      id: string.isRequired,
      name: string.isRequired,
      value: string.isRequired,
    }).isRequired
  ),
  prefix: string,
  suffix: string,
  lineGraph: bool,
  staticLegendHeight: number,
}

export default SingleStat
