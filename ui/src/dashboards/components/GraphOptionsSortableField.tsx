import React, {PureComponent} from 'react'
import {findDOMNode} from 'react-dom'
import {DragSource, DropTarget} from 'react-dnd'
import GraphOptionsCustomizableField from 'src/dashboards/components/GraphOptionsCustomizableField'

const FieldType = 'field'

const cardSource = {
  beginDrag(props) {
    return {
      id: props.id,
      index: props.index,
    }
  },
}

const cardTarget = {
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

interface Props {
  internalName: string
  displayName: string
  index: number
  id: string
  key: string
  isDragging: boolean
  connectDragSource: any
  connectDropTarget: any
  moveField: (dragIndex: number, hoverIndex: number) => void
}

@DropTarget(FieldType, cardTarget, connect => ({
  connectDropTarget: connect.dropTarget(),
}))
@DragSource(FieldType, cardSource, (connect, monitor) => ({
  connectDragSource: connect.dragSource(),
  isDragging: monitor.isDragging(),
}))
export default class GraphOptionsSortableField extends PureComponent<Props> {
  render() {
    const {
      internalName,
      displayName,
      isDragging,
      connectDragSource,
      connectDropTarget,
    } = this.props
    const opacity = isDragging ? 0 : 1

    return connectDragSource(
      connectDropTarget(
        <div>
          <GraphOptionsCustomizableField
            internalName={internalName}
            displayName={displayName}
            visible={true}
          />
        </div>
      )
    )
  }
}
