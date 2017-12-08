import React, {PropTypes, Component} from 'react'
import classnames from 'classnames'
import shallowCompare from 'react-addons-shallow-compare'
import lastValues from 'shared/parsing/lastValues'

import {SMALL_CELL_HEIGHT} from 'src/shared/graphs/helpers'

class SingleStat extends PureComponent {
  render() {
    const {data, cellHeight, isFetchingInitially} = this.props

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

    return (
      <div className="single-stat">
        <span
          className={classnames('single-stat--value', {
            'single-stat--small': cellHeight === SMALL_CELL_HEIGHT,
          })}
        >
          {roundedValue}
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
}

export default SingleStat
