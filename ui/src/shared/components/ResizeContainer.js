import React, {PropTypes} from 'react';
import ResizeHandle from 'shared/components/ResizeHandle';

const {node} = PropTypes;
const ResizeContainer = React.createClass({
  propTypes: {
    children: node.isRequired,
  },

  getInitialState() {
    return {
      leftWidth: '34%',
      rightWidth: '66%',
      isDragging: false,
    };
  },

  handleStopDrag() {
    this.setState({isDragging: false});
  },

  handleStartDrag() {
    this.setState({isDragging: true});
  },

  handleMouseLeave() {
    this.setState({isDragging: false});
  },

  handleDrag(e) {
    if (!this.state.isDragging) {
      return;
    }

    const appWidth = parseInt(getComputedStyle(this.refs.resizeContainer).width, 10);
    // handleOffSet moves the resize handle as many pixels as the side bar is taking up.
    const handleOffSet = window.innerWidth - appWidth;
    const turnToPercent = 100;
    const newLeftPanelPercent = Math.ceil(((e.pageX - handleOffSet) / (appWidth)) * turnToPercent);
    const newRightPanelPercent = (turnToPercent - newLeftPanelPercent);

    // Don't trigger a resize unless the change in size is greater than minResizePercentage
    const minResizePercentage = 0.5;
    if (Math.abs(newLeftPanelPercent - parseFloat(this.state.leftWidth)) < minResizePercentage) {
      return;
    }

    // Don't trigger a resize if the new sizes are too small
    const minLeftPanelWidth = 371;
    const minRightPanelWidth = 389;
    if (((newLeftPanelPercent / turnToPercent) * appWidth) < minLeftPanelWidth || ((newRightPanelPercent / turnToPercent) * appWidth) < minRightPanelWidth) {
      return;
    }

    this.setState({leftWidth: `${(newLeftPanelPercent)}%`, rightWidth: `${(newRightPanelPercent)}%`});
  },

  render() {
    const {leftWidth, rightWidth, isDragging} = this.state;
    const left = React.cloneElement(this.props.children[0], {width: leftWidth});
    const right = React.cloneElement(this.props.children[1], {width: rightWidth});
    const handle = <ResizeHandle isDragging={isDragging} onHandleStartDrag={this.handleStartDrag} />;

    return (
      <div className="resize-container page-contents" onMouseLeave={this.handleMouseLeave} onMouseUp={this.handleStopDrag} onMouseMove={this.handleDrag} ref="resizeContainer" >
        {left}
        {handle}
        {right}
      </div>
    );
  },
});

export default ResizeContainer;
