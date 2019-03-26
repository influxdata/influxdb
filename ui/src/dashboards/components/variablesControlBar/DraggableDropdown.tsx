// Libraries
import * as React from 'react'
import {
  DragSource,
  DropTarget,
  ConnectDropTarget,
  ConnectDragSource,
  DropTargetConnector,
  DragSourceConnector,
  DragSourceMonitor,
} from 'react-dnd'
import classnames from 'classnames'

// Components
import VariableDropdown from './VariableDropdown'

// Constants
const dropdownType = 'dropdown'

const dropdownSource = {
  beginDrag(props: Props) {
    return {
      id: props.id,
      index: props.index,
    }
  },
}

interface Props {
  id: string
  index: number
  name: string
  moveDropdown: (dragIndex: number, hoverIndex: number) => void
  dashboardID: string
}

interface DropdownSourceCollectedProps {
  isDragging: boolean
  connectDragSource: ConnectDragSource
}

interface DropdownTargetCollectedProps {
  connectDropTarget?: ConnectDropTarget
}

const dropdownTarget = {
  hover(props, monitor, component) {
    if (!component) {
      return null
    }
    const dragIndex = monitor.getItem().index
    const hoverIndex = props.index

    // Don't replace items with themselves
    if (dragIndex === hoverIndex) {
      return
    }

    // Time to actually perform the action
    props.moveDropdown(dragIndex, hoverIndex)

    monitor.getItem().index = hoverIndex
  },
}

class Dropdown extends React.Component<
  Props & DropdownSourceCollectedProps & DropdownTargetCollectedProps
> {
  public render() {
    const {
      name,
      id,
      dashboardID,
      isDragging,
      connectDragSource,
      connectDropTarget,
    } = this.props

    const className = classnames('variable-dropdown', {
      'variable-dropdown__dragging': isDragging,
    })

    return connectDragSource(
      connectDropTarget(
        <div className="variable-dropdown--container">
          <div className={className}>
            {/* TODO: Add variable description to title attribute when it is ready */}
            <div className="variable-dropdown--label">
              <div className="customizable-field--drag">
                <span className="hamburger" />
              </div>
              <span>{name}</span>
            </div>
            <div className="variable-dropdown--placeholder" />
            <VariableDropdown variableID={id} dashboardID={dashboardID} />
          </div>
        </div>
      )
    )
  }
}

export default DropTarget<Props & DropdownTargetCollectedProps>(
  dropdownType,
  dropdownTarget,
  (connect: DropTargetConnector) => ({
    connectDropTarget: connect.dropTarget(),
  })
)(
  DragSource<Props & DropdownSourceCollectedProps>(
    dropdownType,
    dropdownSource,
    (connect: DragSourceConnector, monitor: DragSourceMonitor) => ({
      connectDragSource: connect.dragSource(),
      isDragging: monitor.isDragging(),
    })
  )(Dropdown)
)
