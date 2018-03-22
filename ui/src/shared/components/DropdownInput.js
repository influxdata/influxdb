import React from 'react'
import PropTypes from 'prop-types'

const disabledClass = disabled => (disabled ? ' disabled' : '')

const DropdownInput = ({
  searchTerm,
  buttonSize,
  buttonColor,
  toggleStyle,
  disabled,
  onFilterChange,
  onFilterKeyPress,
}) => (
  <div
    className={`dropdown-autocomplete dropdown-toggle ${buttonSize} ${buttonColor}${disabledClass(
      disabled
    )}`}
    style={toggleStyle}
  >
    <input
      className="dropdown-autocomplete--input"
      type="text"
      autoFocus={true}
      placeholder="Filter items..."
      spellCheck={false}
      onChange={onFilterChange}
      onKeyDown={onFilterKeyPress}
      value={searchTerm}
    />
    <span className="caret" />
  </div>
)

export default DropdownInput

const {bool, func, shape, string} = PropTypes

DropdownInput.propTypes = {
  searchTerm: string,
  buttonSize: string,
  buttonColor: string,
  toggleStyle: shape({}),
  disabled: bool,
  onFilterChange: func.isRequired,
  onFilterKeyPress: func.isRequired,
}
