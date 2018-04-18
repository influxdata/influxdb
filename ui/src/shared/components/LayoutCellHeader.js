import React from 'react'
import PropTypes from 'prop-types'
import {isCellUntitled} from 'src/dashboards/utils/cellGetters'

const LayoutCellHeader = ({isEditable, cellName}) => {
  const headingClass = `dash-graph--heading ${
    isEditable ? 'dash-graph--draggable dash-graph--heading-draggable' : ''
  }`

  return (
    <div className={headingClass}>
      <span
        className={
          isCellUntitled(cellName)
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
