import React from 'react'
import classnames from 'classnames'

const {func, bool, string} = React.PropTypes
const ResizeHandle = React.createClass({
  propTypes: {
    onHandleStartDrag: func.isRequired,
    isDragging: bool.isRequired,
    top: string,
  },

  render() {
    const {isDragging, onHandleStartDrag, top} = this.props

    return (
      <div
        className={classnames('resizer--handle', {dragging: isDragging})}
        onMouseDown={onHandleStartDrag}
        style={{top}}
      />
    )
  },
})

export default ResizeHandle
