import React, {PureComponent} from 'react'
import classnames from 'classnames'

import {
  ORIENTATION_VERTICAL,
  ORIENTATION_HORIZONTAL,
} from 'src/shared/constants/'

interface Props {
  onHandleStartDrag: () => void
  isDragging: boolean
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
      />
    )
  }
}

export default ResizeHandle
