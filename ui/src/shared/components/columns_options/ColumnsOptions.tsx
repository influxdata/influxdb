// Libraries
import React, {Component} from 'react'
import {DragDropContext} from 'react-dnd'
import HTML5Backend from 'react-dnd-html5-backend'

// Components
import {Form, EmptyState, Grid} from '@influxdata/clockface'
import DraggableColumn from 'src/shared/components/draggable_column/DraggableColumn'

// Types
import {FieldOption} from 'src/types/dashboards'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  className?: string
  columns: FieldOption[]
  onMoveColumn: (dragIndex: number, hoverIndex: number) => void
  onUpdateColumn: (column: FieldOption) => void
}

class ColumnsOptions extends Component<Props> {
  public render() {
    const {className} = this.props

    return (
      <Grid.Column>
        <Form.Element label="Table Columns">
          <div className={className}>{this.draggableColumns}</div>
        </Form.Element>
      </Grid.Column>
    )
  }

  private get draggableColumns(): JSX.Element | JSX.Element[] {
    const {columns, onMoveColumn, onUpdateColumn} = this.props

    if (columns.length) {
      return columns.map((column: FieldOption, i: number) => (
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
      ))
    }

    return (
      <Form.Box>
        <EmptyState size={ComponentSize.Small}>
          <EmptyState.Text text="This query returned no columns" />
        </EmptyState>
      </Form.Box>
    )
  }
}

export default DragDropContext(HTML5Backend)(ColumnsOptions)
