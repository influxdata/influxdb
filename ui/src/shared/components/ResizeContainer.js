import React, {PropTypes} from 'react';
import ResizeHandle from 'shared/components/ResizeHandle';

const {
  node,
  string,
} = PropTypes;

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
    const minBottomPanelHeight = 200;
    const topHeightPixels = ((newTopPanelPercent / turnToPercent) * appHeight);
    const bottomHeightPixels = ((newBottomPanelPercent / turnToPercent) * appHeight);

    if (topHeightPixels < minTopPanelHeight || bottomHeightPixels < minBottomPanelHeight) {
      return;
    }

    this.setState({topHeight: `${(newTopPanelPercent)}%`, bottomHeight: `${(newBottomPanelPercent)}%`, topHeightPixels});
  },

  renderHandle() {
    const {isDragging, topHeight} = this.state
    return (
      <ResizeHandle isDragging={isDragging} onHandleStartDrag={this.handleStartDrag} top={topHeight} />
    )
  },

  render() {
    const {topHeight, topHeightPixels, bottomHeight} = this.state
    const top = React.cloneElement(this.props.children[0], {height: topHeight, heightPixels: topHeightPixels})
    const bottom = React.cloneElement(this.props.children[1], {height: bottomHeight})
    return (
      <div className="resize-container page-contents" onMouseLeave={this.handleMouseLeave} onMouseUp={this.handleStopDrag} onMouseMove={this.handleDrag} ref="resizeContainer" >
        {top}
        {this.renderHandle()}
        {bottom}
      </div>
    )
  },
})

const ResizeBottom = (props) => (
  <div className="resize-bottom" style={{height: props.height}}>
    {props.children}
  </div>
)

ResizeBottom.propTypes = {
  children: node.isRequired,
  height: string,
}

export {ResizeBottom}

export default ResizeContainer
