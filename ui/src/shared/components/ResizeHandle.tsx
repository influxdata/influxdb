import React, {PureComponent, MouseEvent} from 'react'
import classnames from 'classnames'

import {
  HANDLE_VERTICAL,
  HANDLE_HORIZONTAL,
  HUNDRED,
} from 'src/shared/constants/'

interface Props {
  onStartDrag: (e: MouseEvent<HTMLElement>) => void
  isDragging: boolean
  orientation: string
  percent: number
}

class ResizeHandle extends PureComponent<Props> {
  public render() {
    return (
      <div
        className={this.className}
        style={this.style}
        onMouseDown={this.handleMouseDown}
      />
    )
  }

  private get style() {
    const {percent, orientation} = this.props
    const size = `${percent * HUNDRED}%`

    if (orientation === HANDLE_VERTICAL) {
      return {left: size}
    }

    return {top: size}
  }

  private get className(): string {
    const {isDragging, orientation} = this.props

    return classnames('resizer--handle', {
      dragging: isDragging,
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }

  public handleMouseDown = (e: MouseEvent<HTMLElement>): void => {
    this.props.onStartDrag(e)
  }
}

export default ResizeHandle
