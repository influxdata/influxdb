import React, {Component, ReactNode} from 'react'
import classnames from 'classnames'

import ResizeHandle from 'src/shared/components/ResizeHandle'
import {ErrorHandling} from 'src/shared/decorators/errors'

const hundred = 100
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
  renderTop: (height: number) => ReactNode
  renderBottom: (height: number, top: number) => ReactNode
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
    const {
      containerClass,
      children,
      theme,
      renderTop,
      renderBottom,
    } = this.props

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
          style={this.topStyle}
          ref={r => (this.topRef = r)}
        >
          {renderTop(topHeightPixels)}
        </div>
        <ResizeHandle
          theme={theme}
          isDragging={isDragging}
          onHandleStartDrag={this.handleStartDrag}
          top={this.topHandle}
        />
        <div
          className="resize--bottom"
          style={this.bottomStyle}
          ref={r => (this.bottomRef = r)}
        >
          {renderBottom(topHeightPixels, bottomHeightPixels)}
        </div>
      </div>
    )
  }

  private get topStyle() {
    const {topHeight} = this.state

    return {height: `${topHeight}%`}
  }

  private get bottomStyle() {
    const {topHeight, bottomHeight} = this.state

    return {top: `${topHeight}%`, bottom: `${bottomHeight}%`}
  }

  private get topHandle() {
    const {topHeight} = this.state

    return `${topHeight}%`
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
    const {height} = getComputedStyle(this.containerRef)
    const containerHeight = parseInt(height, 10)
    // verticalOffset moves the resize handle as many pixels as the page-heading is taking up.
    const verticalOffset = window.innerHeight - containerHeight
    const newTopPanelPercent = Math.ceil(
      (e.pageY - verticalOffset) / containerHeight * hundred
    )
    const newBottomPanelPercent = hundred - newTopPanelPercent

    // Don't trigger a resize unless the change in size is greater than minResizePercentage
    const minResizePercentage = 0.5
    if (
      Math.abs(newTopPanelPercent - this.state.topHeight) < minResizePercentage
    ) {
      return
    }

    const topHeightPixels = newTopPanelPercent / hundred * containerHeight
    const bottomHeightPixels = newBottomPanelPercent / hundred * containerHeight

    // Don't trigger a resize if the new sizes are too small
    if (
      topHeightPixels < minTopHeight ||
      bottomHeightPixels < minBottomHeight
    ) {
      return
    }

    this.setState({
      topHeight: newTopPanelPercent,
      topHeightPixels,
      bottomHeight: newBottomPanelPercent,
      bottomHeightPixels,
    })
  }
}

export default ResizeContainer
