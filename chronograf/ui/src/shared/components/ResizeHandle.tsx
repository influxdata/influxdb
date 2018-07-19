import React, {SFC} from 'react'
import classnames from 'classnames'

interface Props {
  onHandleStartDrag: () => void
  isDragging: boolean
  theme?: string
  top?: string
}

const ResizeHandle: SFC<Props> = ({
  onHandleStartDrag,
  isDragging,
  theme,
  top,
}) => (
  <div
    className={classnames('resizer--handle', {
      dragging: isDragging,
      'resizer--malachite': theme === 'kapacitor',
    })}
    onMouseDown={onHandleStartDrag}
    style={{top}}
  />
)

export default ResizeHandle
