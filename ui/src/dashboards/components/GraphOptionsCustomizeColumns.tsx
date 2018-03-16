import React, {SFC} from 'react'

import GraphOptionsCustomizableColumn from 'src/dashboards/components/GraphOptionsCustomizableColumn'
import uuid from 'uuid'

type Column = {
  internalName: string
  displayName: string
  visible: boolean
}

interface Props {
  columns: Column[]
  onColumnRename: (column: Column) => void
}

const GraphOptionsCustomizeColumns: SFC<Props> = ({
  columns,
  onColumnRename,
}) => {
  return (
    <div className="graph-options-group">
      <label className="form-label">Customize Columns</label>
      {columns.map(col => {
        return (
          <GraphOptionsCustomizableColumn
            key={uuid.v4()}
            internalName={col.internalName}
            displayName={col.displayName}
            visible={col.visible}
            onColumnRename={onColumnRename}
          />
        )
      })}
    </div>
  )
}

export default GraphOptionsCustomizeColumns
