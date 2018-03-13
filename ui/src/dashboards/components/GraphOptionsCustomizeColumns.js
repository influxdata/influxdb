import React from 'react'
import PropTypes from 'prop-types'

import GraphOptionsCustomizableColumn from 'src/dashboards/components/GraphOptionsCustomizableColumn'
import uuid from 'uuid'

const GraphOptionsCustomizeColumns = ({columns, onColumnRename}) => {
  return (
    <div className="graph-options-group">
      <label className="form-label">Customize Columns</label>
      {columns.map(col => {
        return (
          <GraphOptionsCustomizableColumn
            key={uuid.v4()}
            originalColumnName={col.name}
            newColumnName={col.newName}
            onColumnRename={onColumnRename}
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
      name: string,
      newName: string,
    })
  ),
  onColumnRename: func,
}

export default GraphOptionsCustomizeColumns
