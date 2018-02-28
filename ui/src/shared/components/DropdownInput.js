import React from 'react'
import PropTypes from 'prop-types'

const DropdownInput = ({
  searchTerm,
  buttonSize,
  buttonColor,
  toggleStyle,
  disabledClass,
  onFilterChange,
  onFilterKeyPress,
}) =>
  <div
    className={`dropdown-autocomplete dropdown-toggle ${buttonSize} ${buttonColor} ${disabledClass}`}
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

export default DropdownInput

const {func, shape, string} = PropTypes

DropdownInput.propTypes = {
  searchTerm: string,
  buttonSize: string,
  buttonColor: string,
  toggleStyle: shape({}),
  disabledClass: string,
  onFilterChange: func,
  onFilterKeyPress: func,
}
