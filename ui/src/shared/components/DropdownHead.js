import React from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'

const disabledClass = disabled => (disabled ? ' disabled' : '')

const DropdownHead = ({
  iconName,
  selected,
  buttonSize,
  toggleStyle,
  buttonColor,
  disabled,
}) => (
  <div
    className={`btn dropdown-toggle ${buttonSize} ${buttonColor}${disabledClass(
      disabled
    )}`}
    style={toggleStyle}
  >
    {iconName && <span className={classnames('icon', {[iconName]: true})} />}
    <span className="dropdown-selected">{selected}</span>
    <span className="caret" />
  </div>
)

const {bool, string, shape} = PropTypes

DropdownHead.propTypes = {
  iconName: string,
  selected: string.isRequired,
  buttonSize: string.isRequired,
  toggleStyle: shape(),
  buttonColor: string.isRequired,
  disabled: bool,
}

export default DropdownHead
