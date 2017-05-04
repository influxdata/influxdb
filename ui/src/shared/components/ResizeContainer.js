import React, {PropTypes} from 'react'
import ResizeHandle from 'shared/components/ResizeHandle'
import classnames from 'classnames'

const {node, number, string} = PropTypes

const maximumNumChildren = 2

const ResizeContainer = React.createClass({
  propTypes: {
    children: node.isRequired,
    containerClass: string.isRequired,
    minTopHeight: number,
    minBottomHeight: number,
  },

  getDefaultProps() {
    return {
      minTopHeight: 200,
      minBottomHeight: 200,
    }
  },

  getInitialState() {
    return {
      topHeight: '60%',
      bottomHeight: '40%',
      isDragging: false,
    }
  },

  handleStopDrag() {
    this.setState({isDragging: false})
  },

  handleStartDrag() {
    this.setState({isDragging: true})
  },

  handleMouseLeave() {
    this.setState({isDragging: false})
  },

  handleDrag(e) {
    if (!this.state.isDragging) {
      return
    }

    const {minTopHeight, minBottomHeight} = this.props
    const oneHundred = 100
    const containerHeight = parseInt(
      getComputedStyle(this.refs.resizeContainer).height,
      10
    )
    // verticalOffset moves the resize handle as many pixels as the page-heading is taking up.
    const verticalOffset = window.innerHeight - containerHeight
    const newTopPanelPercent = Math.ceil(
      (e.pageY - verticalOffset) / containerHeight * oneHundred
    )
    const newBottomPanelPercent = oneHundred - newTopPanelPercent

    // Don't trigger a resize unless the change in size is greater than minResizePercentage
    const minResizePercentage = 0.5
    if (
      Math.abs(newTopPanelPercent - parseFloat(this.state.topHeight)) <
      minResizePercentage
    ) {
      return
    }

    const topHeightPixels = newTopPanelPercent / oneHundred * containerHeight
    const bottomHeightPixels = newBottomPanelPercent / oneHundred * containerHeight

    // Don't trigger a resize if the new sizes are too small
    if (
      topHeightPixels < minTopHeight ||
      bottomHeightPixels < minBottomHeight
    ) {
      return
    }

    this.setState({
      topHeight: `${newTopPanelPercent}%`,
      bottomHeight: `${newBottomPanelPercent}%`,
    })
  },

  renderHandle() {
    const {isDragging, topHeight} = this.state
    return (
      <ResizeHandle
        isDragging={isDragging}
        onHandleStartDrag={this.handleStartDrag}
        top={topHeight}
      />
    )
  },

  render() {
    const {topHeight, bottomHeight, isDragging} = this.state
    const {containerClass, children} = this.props

    if (children.length > maximumNumChildren) {
      console.error(`There cannot be more than ${maximumNumChildren}' children in ResizeContainer`)
      return
    }

    return (
      <div
        className={classnames(`resize--container ${containerClass}`, {'resize--dragging': isDragging})}
        onMouseLeave={this.handleMouseLeave}
        onMouseUp={this.handleStopDrag}
        onMouseMove={this.handleDrag}
        ref="resizeContainer"
      >
        <div className="resize--top" style={{height: topHeight}}>
          {React.cloneElement(children[0])}
        </div>
        {this.renderHandle()}
        <div className="resize--bottom" style={{height: bottomHeight, top: topHeight}}>
          {React.cloneElement(children[1])}
        </div>
      </div>
    )
  },
})

export default ResizeContainer
