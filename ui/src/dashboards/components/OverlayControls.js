import React, {PropTypes} from 'react'
import classnames from 'classnames'

import ConfirmButtons from 'shared/components/ConfirmButtons'

import graphTypes from 'hson!shared/data/graphTypes.hson'

const OverlayControls = props => {
  const {
    onCancel,
    onSave,
    selectedGraphType,
    onSelectGraphType,
    isSavable,
  } = props
  return (
    <div className="overlay-controls">
      <h3 className="overlay--graph-name">Cell Editor</h3>
      <div className="overlay-controls--right">
        <p>Visualization Type:</p>
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
        <ConfirmButtons
          onCancel={onCancel}
          onConfirm={onSave}
          isDisabled={!isSavable}
        />
      </div>
    </div>
  )
}

const {func, string, bool} = PropTypes

OverlayControls.propTypes = {
  onCancel: func.isRequired,
  onSave: func.isRequired,
  selectedGraphType: string.isRequired,
  onSelectGraphType: func.isRequired,
  isSavable: bool,
}

export default OverlayControls
