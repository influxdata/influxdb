import React, {PropTypes} from 'react'
import classnames from 'classnames'

import ConfirmButtons from 'shared/components/ConfirmButtons'

const OverlayControls = ({
  onCancel,
  onSave,
  isDisplayOptionsTabOpen,
  onSelectDisplayOptions,
  isSavable,
}) =>
  <div className="overlay-controls">
    <h3 className="overlay--graph-name">Cell Editor</h3>
    <div className="overlay-controls--right">
      <ul className="nav nav-tablist nav-tablist-sm">
        <li
          key="queries"
          className={classnames({
            active: !isDisplayOptionsTabOpen,
          })}
          onClick={onSelectDisplayOptions(false)}
        >
          Queries
        </li>
        <li
          key="displayOptions"
          className={classnames({
            active: isDisplayOptionsTabOpen,
          })}
          onClick={onSelectDisplayOptions(true)}
        >
          Display Options
        </li>
      </ul>
      <ConfirmButtons
        onCancel={onCancel}
        onConfirm={onSave}
        isDisabled={!isSavable}
      />
    </div>
  </div>

const {func, bool} = PropTypes

OverlayControls.propTypes = {
  onCancel: func.isRequired,
  onSave: func.isRequired,
  isDisplayOptionsTabOpen: bool.isRequired,
  onSelectDisplayOptions: func.isRequired,
  isSavable: bool,
}

export default OverlayControls
