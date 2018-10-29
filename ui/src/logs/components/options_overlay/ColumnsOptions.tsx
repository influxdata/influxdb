import React, {Component} from 'react'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'

import DraggableColumn from 'src/logs/components/draggable_column/DraggableColumn'

import {LogsTableColumn} from 'src/types/logs'

interface Props {
  columns: LogsTableColumn[]
  onMoveColumn: (dragIndex: number, hoverIndex: number) => void
  onUpdateColumn: (column: LogsTableColumn) => void
}

class ColumnsOptions extends Component<Props> {
  public render() {
    const {columns} = this.props

    return (
      <>
        <label className="form-label">Table Columns</label>
        <div className="logs-options--columns">
          {columns.map((c, i) => this.getDraggableColumn(c, i))}
        </div>
      </>
    )
  }

  private getDraggableColumn(column: LogsTableColumn, i: number): JSX.Element {
    const {onMoveColumn, onUpdateColumn} = this.props
    if (column.internalName !== 'time') {
      return (
        <DraggableColumn
          key={column.internalName}
          index={i}
          id={column.internalName}
          internalName={column.internalName}
          displayName={column.displayName}
          visible={column.visible}
          onUpdateColumn={onUpdateColumn}
          onMoveColumn={onMoveColumn}
        />
      )
    }
  }
}

export default DragDropContext(HTML5Backend)(ColumnsOptions)
