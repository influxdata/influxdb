import React from 'react'
import PropTypes from 'prop-types'

import {
  THRESHOLD_TYPE_BG,
  THRESHOLD_TYPE_TEXT,
} from 'shared/constants/thresholds'

// TODO: Needs major refactoring to make thresholds a much more general component to be shared between single stat, gauge, and table.
const GraphOptionsTextWrapping = ({
  thresholdsListType,
  onToggleTextWrapping,
}) => {
  return (
    <div className="form-group col-xs-12">
      <label>Text Wrapping</label>
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          className={`${thresholdsListType === THRESHOLD_TYPE_BG
            ? 'active'
            : ''}`}
          onClick={onToggleTextWrapping}
        >
          Truncate
        </li>
        <li
          className={`${thresholdsListType === THRESHOLD_TYPE_TEXT
            ? 'active'
            : ''}`}
          onClick={onToggleTextWrapping}
        >
          Wrap
        </li>
        <li
          className={`${thresholdsListType === THRESHOLD_TYPE_BG
            ? 'active'
            : ''}`}
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
  thresholdsListType: string,
  onToggleTextWrapping: func,
}

export default GraphOptionsTextWrapping
