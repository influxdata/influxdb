import React, {PropTypes} from 'react'
import classnames from 'classnames'

import graphTypes from 'hson!shared/data/graphTypes.hson'

const GraphTypeSelector = ({selectedGraphType, onSelectGraphType}) =>
  <div className="overlay-controls">
    <div className="overlay-controls--right">
      <p>Visualization Type</p>
      <ul className="nav nav-tablist nav-tablist-sm">
        {graphTypes.map(graphType =>
          <li
            key={graphType.type}
            className={classnames({
              active: graphType.type === selectedGraphType,
            })}
            onClick={() => onSelectGraphType(graphType.type)}
          >
            {graphType.menuOption}
          </li>
        )}
      </ul>
    </div>
  </div>

const {func, string} = PropTypes

GraphTypeSelector.propTypes = {
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
}

export default GraphTypeSelector
