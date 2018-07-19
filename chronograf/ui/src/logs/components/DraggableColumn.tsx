import React, {Component, ChangeEvent} from 'react'
import {findDOMNode} from 'react-dom'
import {
  DragSourceSpec,
  DropTargetConnector,
  DragSourceMonitor,
  DragSource,
  DropTarget,
  DragSourceConnector,
  ConnectDragSource,
  ConnectDropTarget,
  ConnectDragPreview,
} from 'react-dnd'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {LogsTableColumn} from 'src/types/logs'

const columnType = 'column'

interface Props {
  internalName: string
  displayName: string
  visible: boolean
  index: number
  id: string
  key: string
  onUpdateColumn: (column: LogsTableColumn) => void
  isDragging?: boolean
  connectDragSource?: ConnectDragSource
  connectDropTarget?: ConnectDropTarget
  connectDragPreview?: ConnectDragPreview
  onMoveColumn: (dragIndex: number, hoverIndex: number) => void
}

const columnSource: DragSourceSpec<Props> = {
  beginDrag(props) {
    return {
      id: props.id,
      index: props.index,
    }
  },
}

const columnTarget = {
  hover(props, monitor, component) {
    const dragIndex = monitor.getItem().index
    const hoverIndex = props.index

    // Don't replace items with themselves
    if (dragIndex === hoverIndex) {
      return
    }

    // Determine rectangle on screen
    const hoverBoundingRect = findDOMNode(component).getBoundingClientRect()

    // Get vertical middle
    const hoverMiddleY = (hoverBoundingRect.bottom - hoverBoundingRect.top) / 2

    // Determine mouse position
    const clientOffset = monitor.getClientOffset()

    // Get pixels to the top
    const hoverClientY = clientOffset.y - hoverBoundingRect.top

    // Only perform the move when the mouse has crossed half of the items height
    // When dragging downwards, only move when the cursor is below 50%
    // When dragging upwards, only move when the cursor is above 50%

    // Dragging downwards
    if (dragIndex < hoverIndex && hoverClientY < hoverMiddleY) {
      return
    }

    // Dragging upwards
    if (dragIndex > hoverIndex && hoverClientY > hoverMiddleY) {
      return
    }
    // Time to actually perform the action
    props.onMoveColumn(dragIndex, hoverIndex)

    // Note: we're mutating the monitor item here!
    // Generally it's better to avoid mutations,
    // but it's good here for the sake of performance
    // to avoid expensive index searches.
    monitor.getItem().index = hoverIndex
  },
}

function ColumnDropTarget(dropColumnType, dropColumnTarget, dropHandler) {
  return target =>
    DropTarget(dropColumnType, dropColumnTarget, dropHandler)(target) as any
}

function ColumnDragSource(dragColumnType, dragColumnSource, dragHandler) {
  return target =>
    DragSource(dragColumnType, dragColumnSource, dragHandler)(target) as any
}

@ErrorHandling
@ColumnDropTarget(columnType, columnTarget, (connect: DropTargetConnector) => ({
  connectDropTarget: connect.dropTarget(),
}))
@ColumnDragSource(
  columnType,
  columnSource,
  (connect: DragSourceConnector, monitor: DragSourceMonitor) => ({
    connectDragSource: connect.dragSource(),
    connectDragPreview: connect.dragPreview(),
    isDragging: monitor.isDragging(),
  })
)
export default class DraggableColumn extends Component<Props> {
  constructor(props) {
    super(props)

    this.handleColumnRename = this.handleColumnRename.bind(this)
    this.handleToggleVisible = this.handleToggleVisible.bind(this)
  }
  public render(): JSX.Element | null {
    const {
      internalName,
      displayName,
      connectDragPreview,
      connectDropTarget,
      visible,
    } = this.props

    return connectDragPreview(
      connectDropTarget(
        <div className={this.columnClassName}>
          <div className={this.labelClassName}>
            {this.dragHandle}
            {this.visibilityToggle}
            <div className="customizable-field--name">{internalName}</div>
          </div>
          <input
            className="form-control input-sm customizable-field--input"
            type="text"
            spellCheck={false}
            id="internalName"
            value={displayName}
            onChange={this.handleColumnRename}
            placeholder={`Rename ${internalName}`}
            disabled={!visible}
          />
        </div>
      )
    )
  }

  private get dragHandle(): JSX.Element {
    const {connectDragSource} = this.props

    return connectDragSource(
      <div className="customizable-field--drag">
        <span className="hamburger" />
      </div>
    )
  }

  private get visibilityToggle(): JSX.Element {
    const {visible, internalName} = this.props

    if (visible) {
      return (
        <div
          className="customizable-field--visibility"
          onClick={this.handleToggleVisible}
          title={`Click to HIDE ${internalName}`}
        >
          <span className="icon eye-open" />
        </div>
      )
    }

    return (
      <div
        className="customizable-field--visibility"
        onClick={this.handleToggleVisible}
        title={`Click to SHOW ${internalName}`}
      >
        <span className="icon eye-closed" />
      </div>
    )
  }

  private get labelClassName(): string {
    const {visible} = this.props

    if (visible) {
      return 'customizable-field--label'
    }

    return 'customizable-field--label__hidden'
  }

  private get columnClassName(): string {
    const {isDragging} = this.props

    if (isDragging) {
      return 'customizable-field dragging'
    }

    return 'customizable-field'
  }

  private handleColumnRename = (e: ChangeEvent<HTMLInputElement>): void => {
    const {onUpdateColumn, internalName, visible} = this.props
    onUpdateColumn({internalName, displayName: e.target.value, visible})
  }

  private handleToggleVisible = (): void => {
    const {onUpdateColumn, internalName, displayName, visible} = this.props
    onUpdateColumn({internalName, displayName, visible: !visible})
  }
}
