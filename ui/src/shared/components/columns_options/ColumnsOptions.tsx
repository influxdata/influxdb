import React, {Component} from 'react'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'

// Components
import DraggableColumn from 'src/shared/components/draggable_column/DraggableColumn'
import {Form} from 'src/clockface'

// Types
import {FieldOption} from 'src/types/v2/dashboards'

interface Props {
  className?: string
  columns: FieldOption[]
  onMoveColumn: (dragIndex: number, hoverIndex: number) => void
  onUpdateColumn: (column: FieldOption) => void
}

class ColumnsOptions extends Component<Props> {
  public render() {
    const {columns, className} = this.props

    return (
      <Form.Element label="Table Columns">
        <div className={className}>
          {columns.map((c, i) => this.getDraggableColumn(c, i))}
        </div>
      </Form.Element>
    )
  }

  private getDraggableColumn(column: FieldOption, i: number): JSX.Element {
    const {onMoveColumn, onUpdateColumn} = this.props
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

export default DragDropContext(HTML5Backend)(ColumnsOptions)
