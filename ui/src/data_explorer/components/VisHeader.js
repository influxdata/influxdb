import React, {PropTypes} from 'react'
import classnames from 'classnames'
import _ from 'lodash'

const VisHeader = ({views, view, onToggleView, name}) =>
  <div className="graph-heading">
    {views.length
      ? <ul className="nav nav-tablist nav-tablist-sm">
          {views.map(v =>
            <li
              key={v}
              onClick={onToggleView(v)}
              className={classnames({active: view === v})}
              data-test={`data-${v}`}
            >
              {_.upperFirst(v)}
            </li>
          )}
        </ul>
      : null}
    <div className="graph-title">
      {name}
    </div>
  </div>

const {arrayOf, func, string} = PropTypes

VisHeader.propTypes = {
  views: arrayOf(string).isRequired,
  view: string.isRequired,
  onToggleView: func.isRequired,
  name: string.isRequired,
}

export default VisHeader
