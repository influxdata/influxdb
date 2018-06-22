import React, {Component} from 'react'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'
import DraggableColumn from 'src/logs/components/DraggableColumn'
import {LogsTableColumn} from 'src/types/logs'

interface Props {
  columns: LogsTableColumn[]
  onMoveColumn: (dragIndex: number, hoverIndex: number) => void
  onUpdateColumn: (column: LogsTableColumn) => void
}

class ColumnsOptions extends Component<Props> {
  public render() {
    const {columns, onMoveColumn, onUpdateColumn} = this.props

    return (
      <>
        <label className="form-label">Table Columns</label>
        <div className="logs-options--columns">
          {columns.map((c, i) => (
            <DraggableColumn
              key={c.internalName}
              index={i}
              id={c.internalName}
              internalName={c.internalName}
              displayName={c.displayName}
              visible={c.visible}
              onUpdateColumn={onUpdateColumn}
              onMoveColumn={onMoveColumn}
            />
          ))}
        </div>
      </>
    )
  }
}

export default DragDropContext(HTML5Backend)(ColumnsOptions)
