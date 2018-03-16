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
  onColumnUpdate: (column: Column) => void
}

const GraphOptionsCustomizeColumns: SFC<Props> = ({
  columns,
  onColumnUpdate,
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
            onColumnUpdate={onColumnUpdate}
          />
        )
      })}
    </div>
  )
}

export default GraphOptionsCustomizeColumns
