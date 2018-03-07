import React, {PropTypes} from 'react'

import {
  SINGLE_STAT_BG,
  SINGLE_STAT_TEXT,
} from 'src/dashboards/constants/gaugeColors'

// TODO: Needs major refactoring to make thresholds a much more general component to be shared between single stat, gauge, and table.
const GraphOptionsTextWrapping = ({singleStatType, onToggleTextWrapping}) => {
  return (
    <div className="form-group col-xs-12">
      <label>Text Wrapping</label>
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          className={`${singleStatType === SINGLE_STAT_BG ? 'active' : ''}`}
          onClick={onToggleTextWrapping}
        >
          Truncate
        </li>
        <li
          className={`${singleStatType === SINGLE_STAT_TEXT ? 'active' : ''}`}
          onClick={onToggleTextWrapping}
        >
          Wrap
        </li>
        <li
          className={`${singleStatType === SINGLE_STAT_BG ? 'active' : ''}`}
          onClick={onToggleTextWrapping}
        >
          Single Line
        </li>
      </ul>
    </div>
  )
}
const {func, string} = PropTypes

GraphOptionsTextWrapping.propTypes = {
  singleStatType: string,
  onToggleTextWrapping: func,
}

export default GraphOptionsTextWrapping
