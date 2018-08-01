import React, {SFC} from 'react'
import classnames from 'classnames'

interface Props {
  onHandleStartDrag: () => void
  isDragging: boolean
  top?: string
}

const ResizeHandle: SFC<Props> = ({onHandleStartDrag, isDragging, top}) => (
  <div
    className={classnames('resizer--handle', {
      dragging: isDragging,
    })}
    onMouseDown={onHandleStartDrag}
    style={{top}}
  />
)

export default ResizeHandle
