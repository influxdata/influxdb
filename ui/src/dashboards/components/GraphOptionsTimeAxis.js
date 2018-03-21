import React from 'react'
import PropTypes from 'prop-types'

const GraphOptionsTimeAxis = ({verticalTimeAxis, onToggleVerticalTimeAxis}) => (
  <div className="form-group col-xs-12 col-sm-6">
    <label>Time Axis</label>
    <ul className="nav nav-tablist nav-tablist-sm">
      <li
        className={verticalTimeAxis ? 'active' : ''}
        onClick={onToggleVerticalTimeAxis(true)}
      >
        Vertical
      </li>
      <li
        className={verticalTimeAxis ? '' : 'active'}
        onClick={onToggleVerticalTimeAxis(false)}
      >
        Horizontal
      </li>
    </ul>
  </div>
)

const {bool, func} = PropTypes

GraphOptionsTimeAxis.propTypes = {
  verticalTimeAxis: bool,
  onToggleVerticalTimeAxis: func,
}

export default GraphOptionsTimeAxis
