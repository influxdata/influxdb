import React from 'react';
import cx from 'classnames';

const {func, bool} = React.PropTypes;
const ResizeHandle = React.createClass({
  propTypes: {
    onHandleStartDrag: func.isRequired,
    isDragging: bool.isRequired,
  },

  render() {
    return <div className={cx("resizer__handle", {dragging: this.props.isDragging})} ref="resizer" onMouseDown={this.props.onHandleStartDrag} />;
  },
});

export default ResizeHandle;
