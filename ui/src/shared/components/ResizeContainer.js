import React, {PropTypes} from 'react';
import ResizeHandle from 'shared/components/ResizeHandle';

const {node} = PropTypes;
const ResizeContainer = React.createClass({
  propTypes: {
    children: node.isRequired,
  },

  getInitialState() {
    return {
      leftWidth: '33.3333%',
      rightWidth: '66.6666%',
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
    const newLeftPanelPercent = ((e.pageX - handleOffSet) / (appWidth));
    const newRightPanelPercent = (1 - ((e.pageX - handleOffSet) / (appWidth)));
    const turnToPercent = 100;

    // dont trigger a resize unless the change in size is greater than minResizePercentage
    const minResizePercentage = 0.5;
    if (Math.abs(newLeftPanelPercent * turnToPercent - parseFloat(this.state.leftWidth)) < minResizePercentage) {
      return;
    }

    this.setState({leftWidth: `${(newLeftPanelPercent * turnToPercent)}%`, rightWidth: `${(newRightPanelPercent * turnToPercent)}%`});
  },

  render() {
    const {leftWidth, rightWidth, isDragging} = this.state;
    const left = React.cloneElement(this.props.children[0], {width: leftWidth});
    const right = React.cloneElement(this.props.children[1], {width: rightWidth});
    const handle = <ResizeHandle isDragging={isDragging} onHandleStartDrag={this.handleStartDrag} />;

    return (
      <div className="resize-container main-content" onMouseLeave={this.handleMouseLeave} onMouseUp={this.handleStopDrag} onMouseMove={this.handleDrag} ref="resizeContainer" >
        {left}
        {handle}
        {right}
      </div>
    );
  },
});

export default ResizeContainer;
