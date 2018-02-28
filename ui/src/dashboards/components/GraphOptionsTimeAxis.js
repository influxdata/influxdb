import React, {PropTypes} from 'react'

const VERTICAL = 'VERTICAL'
const HORIZONTAL = 'HORIZONTAL'
const GraphOptionsTimeAxis = ({TimeAxis, onToggleTimeAxis}) =>
  <div className="form-group col-xs-6">
    <label>Time Axis</label>
    <ul className="nav nav-tablist nav-tablist-sm">
      <li
        className={`${TimeAxis === VERTICAL ? 'active' : ''}`}
        onClick={onToggleTimeAxis}
      >
        Vertical
      </li>
      <li
        className={`${TimeAxis === HORIZONTAL ? 'active' : ''}`}
        onClick={onToggleTimeAxis}
      >
        Horizontal
      </li>
    </ul>
  </div>

const {func, string} = PropTypes

GraphOptionsTimeAxis.propTypes = {TimeAxis: string, onToggleTimeAxis: func}

export default GraphOptionsTimeAxis
