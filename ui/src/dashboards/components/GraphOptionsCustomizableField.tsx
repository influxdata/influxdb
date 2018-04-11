import React, {PureComponent} from 'react'
import {findDOMNode} from 'react-dom'
import {DragSource, DropTarget} from 'react-dnd'

const FieldType = 'field'

const fieldSource = {
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

interface Field {
  internalName: string
  displayName: string
  visible: boolean
  order?: number
}

interface Props {
  internalName: string
  displayName: string
  index: number
  id: string
  key: string
  isDragging: boolean
  onFieldUpdate?: (field: Field) => void
  connectDragSource: any
  connectDropTarget: any
  moveField: (dragIndex: number, hoverIndex: number) => void
}

@DropTarget(FieldType, fieldTarget, connect => ({
  connectDropTarget: connect.dropTarget(),
}))
@DragSource(FieldType, fieldSource, (connect, monitor) => ({
  connectDragSource: connect.dragSource(),
  connectDragPreview: connect.dragPreview(),
  isDragging: monitor.isDragging(),
}))
export default class GraphOptionsCustomizableField extends PureComponent<
  Props
> {
  constructor(props) {
    super(props)

    this.handleFieldRename = this.handleFieldRename.bind(this)
    this.handleToggleVisible = this.handleToggleVisible.bind(this)
  }
  public render() {
    const {
      internalName,
      displayName,
      isDragging,
      onFieldUpdate,
      connectDragSource,
      connectDragPreview,
      connectDropTarget,
    } = this.props
    const visible = true
    const opacity = isDragging ? 0 : 1
    return connectDragPreview(
      connectDropTarget(
        <div className="customizable-field">
          <div
            style={{opacity}}
            className={
              visible
                ? 'customizable-field--label'
                : 'customizable-field--label__hidden'
            }
            onClick={this.handleToggleVisible}
            title={
              visible
                ? `Click to HIDE ${internalName}`
                : `Click to SHOW ${internalName}`
            }
          >
            {connectDragSource(<span className={'icon shuffle'} />)}
            <span className={visible ? 'icon eye-open' : 'icon eye-closed'} />
            {internalName}
          </div>
          <input
            className="customizable-field--input"
            type="text"
            id="internalName"
            value={displayName}
            onBlur={this.handleFieldRename}
            placeholder={`Rename ${internalName}`}
            disabled={!visible}
          />
        </div>
      )
    )
  }

  private handleFieldRename(rename: string) {
    const {onFieldUpdate, internalName, visible} = this.props
    onFieldUpdate({internalName, displayName: rename, visible})
  }

  private handleToggleVisible() {
    const {onFieldUpdate, internalName, displayName, visible} = this.props
    onFieldUpdate({internalName, displayName, visible: !visible})
  }
}
