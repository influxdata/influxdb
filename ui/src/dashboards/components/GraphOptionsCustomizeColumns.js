import React, {PropTypes} from 'react'

import GraphOptionsCustomizableColumn from 'src/dashboards/components/GraphOptionsCustomizableColumn'
import uuid from 'node-uuid'

const GraphOptionsCustomizeColumns = ({columns, handleColumnRename}) => {
  return (
    <div>
      <label>Customize Columns</label>
      {columns.map(col => {
        return (
          <GraphOptionsCustomizableColumn
            key={uuid.v4()}
            originalColumnName={col.name}
            newColumnName={col.newName}
            handleColumnRename={handleColumnRename}
          />
        )
      })}
    </div>
  )
}
const {arrayOf, func, shape, string} = PropTypes

GraphOptionsCustomizeColumns.propTypes = {
  columns: arrayOf(
    shape({
      name: string.isRequired,
      newName: string,
    }).isRequired
  ).isRequired,
  handleColumnRename: func.isRequired,
}

export default GraphOptionsCustomizeColumns
