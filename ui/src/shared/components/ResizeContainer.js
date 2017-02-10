import React, {PropTypes} from 'react';
import ResizeHandle from 'shared/components/ResizeHandle';

const {node} = PropTypes;
const ResizeContainer = React.createClass({
  propTypes: {
    children: node.isRequired,
  },

  getInitialState() {
    return {
      topHeight: '60%',
      bottomHeight: '40%',
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

    const appHeight = parseInt(getComputedStyle(this.refs.resizeContainer).height, 10);
    // headingOffset moves the resize handle as many pixels as the page-heading is taking up.
    const headingOffset = window.innerHeight - appHeight;
    const turnToPercent = 100;
    const newTopPanelPercent = Math.ceil(((e.pageY - headingOffset) / (appHeight)) * turnToPercent);
    const newBottomPanelPercent = (turnToPercent - newTopPanelPercent);

    // Don't trigger a resize unless the change in size is greater than minResizePercentage
    const minResizePercentage = 0.5;
    if (Math.abs(newTopPanelPercent - parseFloat(this.state.topHeight)) < minResizePercentage) {
      return;
    }

    // Don't trigger a resize if the new sizes are too small
    const minTopPanelHeight = 200;
    const minBottomPanelHeight = 100;
    const topHeightPixels = ((newTopPanelPercent / turnToPercent) * appHeight);
    const bottomHeightPixels = ((newBottomPanelPercent / turnToPercent) * appHeight);

    if (topHeightPixels < minTopPanelHeight || bottomHeightPixels < minBottomPanelHeight) {
      return;
    }

    this.setState({topHeight: `${(newTopPanelPercent)}%`, bottomHeight: `${(newBottomPanelPercent)}%`, topHeightPixels});
  },

  render() {
    const {topHeight, bottomHeight, isDragging, topHeightPixels} = this.state;
    const top = React.cloneElement(this.props.children[0], {height: topHeight, heightPixels: topHeightPixels});
    const bottom = React.cloneElement(this.props.children[1], {height: bottomHeight, top: topHeight});
    const handle = <ResizeHandle isDragging={isDragging} onHandleStartDrag={this.handleStartDrag} top={topHeight} />;

    return (
      <div className="resize-container page-contents" onMouseLeave={this.handleMouseLeave} onMouseUp={this.handleStopDrag} onMouseMove={this.handleDrag} ref="resizeContainer" >
        {top}
        {handle}
        {bottom}
      </div>
    );
  },
});

export default ResizeContainer;
