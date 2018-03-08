import React, {PropTypes} from 'react'

import {
  SINGLE_STAT_BG,
  SINGLE_STAT_TEXT,
} from 'src/dashboards/constants/gaugeColors'

// TODO: Needs major refactoring to make thresholds a much more general component to be shared between single stat, gauge, and table.
const GraphOptionsThresholdColoring = ({
  onToggleSingleStatType,
  singleStatType,
}) => {
  return (
    <div className="form-group col-xs-12 col-md-6">
      <label>Threshold Coloring</label>
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          className={`${singleStatType === SINGLE_STAT_BG ? 'active' : ''}`}
          onClick={onToggleSingleStatType(SINGLE_STAT_BG)}
        >
          Background
        </li>
        <li
          className={`${singleStatType === SINGLE_STAT_TEXT ? 'active' : ''}`}
          onClick={onToggleSingleStatType(SINGLE_STAT_TEXT)}
        >
          Text
        </li>
      </ul>
    </div>
  )
}
const {func, string} = PropTypes

GraphOptionsThresholdColoring.propTypes = {
  singleStatType: string,
  onToggleSingleStatType: func,
}

export default GraphOptionsThresholdColoring
