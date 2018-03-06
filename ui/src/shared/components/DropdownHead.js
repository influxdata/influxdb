import React from 'react'
import PropTypes from 'prop-types'
import classnames from 'classnames'

const DropdownHead = ({
  iconName,
  selected,
  buttonSize,
  toggleStyle,
  buttonColor,
  disabledClass,
}) =>
  <div
    className={`btn dropdown-toggle ${buttonSize} ${buttonColor} ${disabledClass}`}
    style={toggleStyle}
  >
    {iconName && <span className={classnames('icon', {[iconName]: true})} />}
    <span className="dropdown-selected">
      {selected}
    </span>
    <span className="caret" />
  </div>

const {string, shape} = PropTypes

DropdownHead.propTypes = {
  iconName: string,
  selected: string,
  buttonSize: string,
  toggleStyle: shape(),
  buttonColor: string,
  disabledClass: string,
}

export default DropdownHead
