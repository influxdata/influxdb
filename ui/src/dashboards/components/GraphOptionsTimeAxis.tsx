import React from 'react'

interface Props {
  verticalTimeAxis: boolean
  onToggleVerticalTimeAxis: (b: boolean) => () => void
}

const GraphOptionsTimeAxis = ({
  verticalTimeAxis,
  onToggleVerticalTimeAxis,
}: Props) => (
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

export default GraphOptionsTimeAxis
