import React from 'react'
import PropTypes from 'prop-types'

import {THRESHOLD_TYPE_BG, THRESHOLD_TYPE_TEXT} from 'shared/constants/thresholds'

// TODO: Needs major refactoring to make thresholds a much more general component to be shared between single stat, gauge, and table.
const GraphOptionsThresholdColoring = ({
  onToggleThresholdsListType,
  thresholdsListType,
}) => {
  return (
    <div className="form-group col-xs-12 col-md-6">
      <label>Threshold Coloring</label>
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          className={`${thresholdsListType === THRESHOLD_TYPE_BG ? 'active' : ''}`}
          onClick={onToggleThresholdsListType(THRESHOLD_TYPE_BG)}
        >
          Background
        </li>
        <li
          className={`${thresholdsListType === THRESHOLD_TYPE_TEXT ? 'active' : ''}`}
          onClick={onToggleThresholdsListType(THRESHOLD_TYPE_TEXT)}
        >
          Text
        </li>
      </ul>
    </div>
  )
}
const {func, string} = PropTypes

GraphOptionsThresholdColoring.propTypes = {
  thresholdsListType: string,
  onToggleThresholdsListType: func,
}

export default GraphOptionsThresholdColoring
