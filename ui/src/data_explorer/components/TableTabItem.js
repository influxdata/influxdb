import React, {PropTypes} from 'react'
import classnames from 'classnames'

const TableTabItem = ({name, index, onClickTab, isActive}) =>
  <div
    className={classnames('table--tab', {active: isActive})}
    onClick={onClickTab(index)}
  >
    {name}
  </div>

const {bool, func, number, string} = PropTypes

TableTabItem.propTypes = {
  name: string,
  onClickTab: func.isRequired,
  index: number.isRequired,
  isActive: bool,
}

export default TableTabItem
