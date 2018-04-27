import React, {Component, ReactNode} from 'react'
import classnames from 'classnames'

import ResizeHandle from 'src/shared/components/ResizeHandle'
import {ErrorHandling} from 'src/shared/decorators/errors'

const maximumNumChildren = 2
const defaultMinTopHeight = 200
const defaultMinBottomHeight = 200
const defaultInitialTopHeight = '50%'
const defaultInitialBottomHeight = '50%'

interface State {
  isDragging: boolean
  topHeight: number
  topHeightPixels: number
  bottomHeight: number
  bottomHeightPixels: number
}

interface Props {
  children: ReactNode
  containerClass: string
  minTopHeight: number
  minBottomHeight: number
  initialTopHeight: string
  initialBottomHeight: string
  theme?: string
}

@ErrorHandling
class ResizeContainer extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    minTopHeight: defaultMinTopHeight,
    minBottomHeight: defaultMinBottomHeight,
    initialTopHeight: defaultInitialTopHeight,
    initialBottomHeight: defaultInitialBottomHeight,
    theme: '',
  }

  private topRef: HTMLElement
  private bottomRef: HTMLElement
  private containerRef: HTMLElement

  constructor(props) {
    super(props)
    this.state = {
      isDragging: false,
      topHeight: props.initialTopHeight,
      topHeightPixels: 0,
      bottomHeight: props.initialBottomHeight,
      bottomHeightPixels: 0,
    }
  }

  public componentDidMount() {
    this.setState({
      bottomHeightPixels: this.bottomRef.getBoundingClientRect().height,
      topHeightPixels: this.topRef.getBoundingClientRect().height,
    })
  }

  public render() {
    const {topHeightPixels, bottomHeightPixels, isDragging} = this.state
    const {containerClass, children, theme} = this.props

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
        ref={r => (this.containerRef = r)}
      >
        <div
          className="resize--top"
          style={{
            height: this.percentageHeights.top,
          }}
          ref={r => (this.topRef = r)}
        >
          {React.cloneElement(children[0], {
            resizerBottomHeight: bottomHeightPixels,
            resizerTopHeight: topHeightPixels,
          })}
        </div>
        <ResizeHandle
          theme={theme}
          isDragging={isDragging}
          onHandleStartDrag={this.handleStartDrag}
          top={this.percentageHeights.top}
        />
        <div
          className="resize--bottom"
          style={{
            height: this.percentageHeights.bottom,
            top: this.percentageHeights.top,
          }}
          ref={r => (this.bottomRef = r)}
        >
          {React.cloneElement(children[1], {
            resizerBottomHeight: bottomHeightPixels,
            resizerTopHeight: topHeightPixels,
          })}
        </div>
      </div>
    )
  }

  private get percentageHeights() {
    const {topHeight, bottomHeight} = this.state

    return {top: `${topHeight}%`, bottom: `${bottomHeight}%`}
  }

  private handleStartDrag = () => {
    this.setState({isDragging: true})
  }

  private handleStopDrag = () => {
    this.setState({isDragging: false})
  }

  private handleMouseLeave = () => {
    this.setState({isDragging: false})
  }

  private handleDrag = e => {
    if (!this.state.isDragging) {
      return
    }

    const {minTopHeight, minBottomHeight} = this.props
    const oneHundred = 100
    const {height} = getComputedStyle(this.containerRef)
    const containerHeight = Number(height)
    // verticalOffset moves the resize handle as many pixels as the page-heading is taking up.
    const verticalOffset = window.innerHeight - containerHeight
    const newTopPanelPercent = Math.ceil(
      (e.pageY - verticalOffset) / containerHeight * oneHundred
    )
    const newBottomPanelPercent = oneHundred - newTopPanelPercent

    // Don't trigger a resize unless the change in size is greater than minResizePercentage
    const minResizePercentage = 0.5
    if (
      Math.abs(newTopPanelPercent - this.state.topHeight) < minResizePercentage
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
      topHeight: newTopPanelPercent,
      bottomHeight: newBottomPanelPercent,
      bottomHeightPixels,
      topHeightPixels,
    })
  }
}

export default ResizeContainer
