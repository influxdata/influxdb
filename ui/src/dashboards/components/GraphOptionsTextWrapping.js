import React, {PropTypes} from 'react'

import {
  SINGLE_STAT_BG,
  SINGLE_STAT_TEXT,
} from 'src/dashboards/constants/gaugeColors'

const GraphOptionsTextWrapping = ({
  singleStatType,
  handleToggleTextWrapping,
}) => {
  return (
    <div>
      <label>Text Wrapping</label>
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          className={`${singleStatType === SINGLE_STAT_BG ? 'active' : ''}`}
          onClick={handleToggleTextWrapping}
        >
          Truncate
        </li>
        <li
          className={`${singleStatType === SINGLE_STAT_TEXT ? 'active' : ''}`}
          onClick={handleToggleTextWrapping}
        >
          Wrap
        </li>
        <li
          className={`${singleStatType === SINGLE_STAT_BG ? 'active' : ''}`}
          onClick={handleToggleTextWrapping}
        >
          Single Line
        </li>
      </ul>
    </div>
  )
}
const {func, string} = PropTypes

GraphOptionsTextWrapping.propTypes = {
  singleStatType: string.isRequired,
  handleToggleTextWrapping: func.isRequired,
}

export default GraphOptionsTextWrapping
