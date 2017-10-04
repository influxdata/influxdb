import React, {PropTypes} from 'react'
import classnames from 'classnames'

import ConfirmButtons from 'shared/components/ConfirmButtons'
import SourceSelector from 'src/dashboards/components/SourceSelector'

const OverlayControls = ({
  onSave,
  sources,
  selected,
  onCancel,
  isSavable,
  onSetQuerySource,
  isDisplayOptionsTabActive,
  onClickDisplayOptions,
}) =>
  <div className="overlay-controls">
    <SourceSelector
      sources={sources}
      selected={selected}
      onSetQuerySource={onSetQuerySource}
    />
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
        Options
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

const {arrayOf, bool, func, shape, string} = PropTypes

OverlayControls.propTypes = {
  onCancel: func.isRequired,
  onSave: func.isRequired,
  isDisplayOptionsTabActive: bool.isRequired,
  onClickDisplayOptions: func.isRequired,
  isSavable: bool,
  sources: arrayOf(shape()),
  onSetQuerySource: func,
  selected: string,
}

export default OverlayControls
