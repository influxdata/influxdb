import React, {SFC} from 'react'
import {isCellUntitled} from 'src/dashboards/utils/cellGetters'

interface Props {
  isEditable: boolean
  cellName: string
}

const LayoutCellHeader: SFC<Props> = ({isEditable, cellName}) => {
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

export default LayoutCellHeader
