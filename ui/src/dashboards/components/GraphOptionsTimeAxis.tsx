import React, {SFC} from 'react'

interface GraphOptionsTimeAxisProps {
  verticalTimeAxis: boolean
  onToggleVerticalTimeAxis: (b: boolean) => () => void
}

const GraphOptionsTimeAxis: SFC<GraphOptionsTimeAxisProps> = ({
  verticalTimeAxis,
  onToggleVerticalTimeAxis,
}) => (
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
