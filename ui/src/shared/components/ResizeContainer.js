import React, {Component, PropTypes} from 'react'
import classnames from 'classnames'

import ResizeHandle from 'shared/components/ResizeHandle'

const maximumNumChildren = 2
const defaultMinTopHeight = 200
const defaultMinBottomHeight = 200
const defaultInitialTopHeight = '50%'
const defaultInitialBottomHeight = '50%'

class ResizeContainer extends Component {
  constructor(props) {
    super(props)
    this.state = {
      isDragging: false,
      topHeight: props.initialTopHeight,
      bottomHeight: props.initialBottomHeight,
    }

    this.handleStartDrag = ::this.handleStartDrag
    this.handleStopDrag = ::this.handleStopDrag
    this.handleMouseLeave = ::this.handleMouseLeave
    this.handleDrag = ::this.handleDrag
  }

  static defaultProps = {
    minTopHeight: defaultMinTopHeight,
    minBottomHeight: defaultMinBottomHeight,
    initialTopHeight: defaultInitialTopHeight,
    initialBottomHeight: defaultInitialBottomHeight,
  }

  componentDidMount() {
    this.setState({
      bottomHeightPixels: this.bottom.getBoundingClientRect().height,
      topHeightPixels: this.top.getBoundingClientRect().height,
    })
  }

  handleStartDrag() {
    this.setState({isDragging: true})
  }

  handleStopDrag() {
    this.setState({isDragging: false})
  }

  handleMouseLeave() {
    this.setState({isDragging: false})
  }

  handleDrag(e) {
    if (!this.state.isDragging) {
      return
    }

    const {minTopHeight, minBottomHeight} = this.props
    const oneHundred = 100
    const containerHeight = parseInt(
      getComputedStyle(this.resizeContainer).height,
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
    const bottomHeightPixels =
      newBottomPanelPercent / oneHundred * containerHeight

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
      bottomHeightPixels,
      topHeightPixels,
    })
  }

  render() {
    const {
      topHeightPixels,
      bottomHeightPixels,
      topHeight,
      bottomHeight,
      isDragging,
    } = this.state
    const {containerClass, children} = this.props

    if (React.Children.count(children) > maximumNumChildren) {
      console.error(
        `There cannot be more than ${maximumNumChildren}' children in ResizeContainer`
      )
      return
    }

    return (
      <div
        className={classnames(`resize--container ${containerClass}`, {
          'resize--dragging': isDragging,
        })}
        onMouseLeave={this.handleMouseLeave}
        onMouseUp={this.handleStopDrag}
        onMouseMove={this.handleDrag}
        ref={r => (this.resizeContainer = r)}
      >
        <div
          className="resize--top"
          style={{height: topHeight}}
          ref={r => (this.top = r)}
        >
          {React.cloneElement(children[0], {
            resizerBottomHeight: bottomHeightPixels,
            resizerTopHeight: topHeightPixels,
          })}
        </div>
        <ResizeHandle
          isDragging={isDragging}
          onHandleStartDrag={this.handleStartDrag}
          top={topHeight}
        />
        <div
          className="resize--bottom"
          style={{height: bottomHeight, top: topHeight}}
          ref={r => (this.bottom = r)}
        >
          {React.cloneElement(children[1], {
            resizerBottomHeight: bottomHeightPixels,
            resizerTopHeight: topHeightPixels,
          })}
        </div>
      </div>
    )
  }
}

const {node, number, string} = PropTypes

ResizeContainer.propTypes = {
  children: node.isRequired,
  containerClass: string.isRequired,
  minTopHeight: number,
  minBottomHeight: number,
  initialTopHeight: string,
  initialBottomHeight: string,
}

export default ResizeContainer
