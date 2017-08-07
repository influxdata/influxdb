import React, {PropTypes} from 'react'
import classnames from 'classnames'

import ConfirmButtons from 'shared/components/ConfirmButtons'

const OverlayControls = ({
  onCancel,
  onSave,
  isDisplayOptionsTabActive,
  onClickDisplayOptions,
  isSavable,
}) =>
  <div className="overlay-controls">
    <h3 className="overlay--graph-name">Cell Editor</h3>
    <ul className="nav nav-tablist nav-tablist-sm">
      <li
        key="queries"
        className={classnames({
          active: !isDisplayOptionsTabActive,
        })}
        onClick={onClickDisplayOptions(false)}
      >
        Queries
      </li>
      <li
        key="displayOptions"
        className={classnames({
          active: isDisplayOptionsTabActive,
        })}
        onClick={onClickDisplayOptions(true)}
      >
        Display Options
      </li>
    </ul>
    <div className="overlay-controls--right">
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
  isDisplayOptionsTabActive: bool.isRequired,
  onClickDisplayOptions: func.isRequired,
  isSavable: bool,
}

export default OverlayControls
