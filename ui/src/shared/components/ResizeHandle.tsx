import React, {PureComponent} from 'react'
import classnames from 'classnames'

import {
  ORIENTATION_VERTICAL,
  ORIENTATION_HORIZONTAL,
  HUNDRED,
} from 'src/shared/constants/'

interface Props {
  onHandleStartDrag: () => void
  isDragging: boolean
  offset: number
  orientation: string
}

class ResizeHandle extends PureComponent<Props> {
  public render() {
    const {onHandleStartDrag, isDragging, orientation} = this.props

    return (
      <div
        className={classnames('resizer--handle', {
          dragging: isDragging,
          vertical: orientation === ORIENTATION_VERTICAL,
          horizontal: orientation === ORIENTATION_HORIZONTAL,
        })}
        onMouseDown={onHandleStartDrag}
        style={this.style}
      />
    )
  }

  private get style() {
    const {orientation, offset} = this.props

    if (orientation === ORIENTATION_VERTICAL) {
      return {left: `${offset * HUNDRED}%`}
    }

    return {top: `${offset * HUNDRED}%`}
  }
}

export default ResizeHandle
