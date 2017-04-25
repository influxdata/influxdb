import React, {PropTypes} from 'react'
import classNames from 'classnames'

const VisHeader = ({views, view, onToggleView, name}) => (
  <div className="graph-heading">
    <div className="graph-actions">
      <ul className="toggle toggle-sm">
        {views.map(v => (
          <li
            key={v}
            onClick={() => onToggleView(v)}
            className={classNames('toggle-btn ', {active: view === v})}>
            {v}
          </li>
        ))}
      </ul>
    </div>
    <div className="graph-title">{name}</div>
  </div>
)

const {
  arrayOf,
  func,
  string,
} = PropTypes

VisHeader.propTypes = {
  views: arrayOf(string).isRequired,
  view: string.isRequired,
  onToggleView: func.isRequired,
  name: string.isRequired,
}

export default VisHeader
