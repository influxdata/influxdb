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

const fieldType = 'field'

interface RenamableField {
  internalName: string
  displayName: string
  visible: boolean
}

interface Props {
  internalName: string
  displayName: string
  visible: boolean
  index: number
  id: string
  key: string
  onFieldUpdate: (field: RenamableField) => void
  isDragging?: boolean
  connectDragSource?: ConnectDragSource
  connectDropTarget?: ConnectDropTarget
  connectDragPreview?: ConnectDragPreview
  moveField: (dragIndex: number, hoverIndex: number) => void
}

const fieldSource: DragSourceSpec<Props> = {
  beginDrag(props) {
    return {
      id: props.id,
      index: props.index,
    }
  },
}

const fieldTarget = {
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
    props.moveField(dragIndex, hoverIndex)

    // Note: we're mutating the monitor item here!
    // Generally it's better to avoid mutations,
    // but it's good here for the sake of performance
    // to avoid expensive index searches.
    monitor.getItem().index = hoverIndex
  },
}

function MyDropTarget(dropv1, dropv2, dropfunc1) {
  return target => DropTarget(dropv1, dropv2, dropfunc1)(target) as any
}

function MyDragSource(dragv1, dragv2, dragfunc1) {
  return target => DragSource(dragv1, dragv2, dragfunc1)(target) as any
}

@ErrorHandling
@MyDropTarget(fieldType, fieldTarget, (connect: DropTargetConnector) => ({
  connectDropTarget: connect.dropTarget(),
}))
@MyDragSource(
  fieldType,
  fieldSource,
  (connect: DragSourceConnector, monitor: DragSourceMonitor) => ({
    connectDragSource: connect.dragSource(),
    connectDragPreview: connect.dragPreview(),
    isDragging: monitor.isDragging(),
  })
)
export default class GraphOptionsCustomizableField extends Component<Props> {
  constructor(props) {
    super(props)

    this.handleFieldRename = this.handleFieldRename.bind(this)
    this.handleToggleVisible = this.handleToggleVisible.bind(this)
  }
  public render(): JSX.Element | null {
    const {
      internalName,
      displayName,
      isDragging,
      connectDragSource,
      connectDragPreview,
      connectDropTarget,
      visible,
    } = this.props

    const fieldClass = `customizable-field${isDragging ? ' dragging' : ''}`
    const labelClass = `${
      visible
        ? 'customizable-field--label'
        : 'customizable-field--label__hidden'
    }`

    const visibilityTitle = visible
      ? `Click to HIDE ${internalName}`
      : `Click to SHOW ${internalName}`

    return connectDragPreview(
      connectDropTarget(
        <div className={fieldClass}>
          <div className={labelClass}>
            {connectDragSource(
              <div className="customizable-field--drag">
                <span className="hamburger" />
              </div>
            )}
            <div
              className="customizable-field--visibility"
              onClick={this.handleToggleVisible}
              title={visibilityTitle}
            >
              <span className={visible ? 'icon eye-open' : 'icon eye-closed'} />
            </div>
            <div className="customizable-field--name">{internalName}</div>
          </div>
          <input
            className="form-control input-sm customizable-field--input"
            type="text"
            spellCheck={false}
            id="internalName"
            value={displayName}
            data-test="custom-time-format"
            onChange={this.handleFieldRename}
            placeholder={`Rename ${internalName}`}
            disabled={!visible}
          />
        </div>
      )
    )
  }

  private handleFieldRename(e: ChangeEvent<HTMLInputElement>) {
    const {onFieldUpdate, internalName, visible} = this.props
    onFieldUpdate({internalName, displayName: e.target.value, visible})
  }

  private handleToggleVisible() {
    const {onFieldUpdate, internalName, displayName, visible} = this.props
    onFieldUpdate({internalName, displayName, visible: !visible})
  }
}
