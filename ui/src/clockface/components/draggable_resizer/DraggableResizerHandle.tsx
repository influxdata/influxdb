// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  id: string
  position: number
  onStartDrag?: (dragIndex: number) => void
  dragging?: boolean
  dragIndex?: number
}

@ErrorHandling
class DraggableResizerHandle extends Component<Props> {
  public render() {
    return (
      <div
        className={this.className}
        onMouseDown={this.handleMouseDown}
        title="Drag to resize"
      />
    )
  }

  private handleMouseDown = () => {
    const {dragIndex, onStartDrag} = this.props

    if (dragIndex !== null) {
      onStartDrag(dragIndex)
    }
  }

  private get className(): string {
    const {dragging} = this.props

    return classnames('draggable-resizer--handle', {
      'draggable-resizer--handle-dragging': dragging,
    })
  }
}

export default DraggableResizerHandle
