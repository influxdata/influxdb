import React, {PropTypes} from 'react'
import {NEW_DEFAULT_DASHBOARD_CELL} from 'src/dashboards/constants/index'

const LayoutCellHeader = ({isEditable, cellName}) => {
  const cellNameIsDefault = cellName === NEW_DEFAULT_DASHBOARD_CELL.name

  const headingClass = `dash-graph--heading ${isEditable
    ? 'dash-graph--draggable dash-graph--heading-draggable'
    : ''}`

  return (
    <div className={headingClass}>
      <span
        className={
          cellNameIsDefault
            ? 'dash-graph--name dash-graph--name__default'
            : 'dash-graph--name'
        }
      >
        {cellName}
      </span>
    </div>
  )
}

const {bool, string} = PropTypes

LayoutCellHeader.propTypes = {
  isEditable: bool,
  cellName: string,
}

export default LayoutCellHeader
