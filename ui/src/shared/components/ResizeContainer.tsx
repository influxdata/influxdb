import React, {Component, ReactNode, MouseEvent} from 'react'
import classnames from 'classnames'

import ResizeHalf from 'src/shared/components/ResizeHalf'
import ResizeHandle from 'src/shared/components/ResizeHandle'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  HANDLE_HORIZONTAL,
  HANDLE_VERTICAL,
  REQUIRED_HALVES,
} from 'src/shared/constants/index'

interface State {
  topPercent: number
  bottomPercent: number
  isDragging: boolean
}

interface Props {
  children: ReactNode
  topMinPixels: number
  bottomMinPixels: number
  orientation?: string
  containerClass: string
}

@ErrorHandling
class Resizer extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    orientation: HANDLE_HORIZONTAL,
  }

  private containerRef: HTMLElement

  constructor(props) {
    super(props)
    this.state = {
      topPercent: 0.5,
      bottomPercent: 0.5,
      isDragging: false,
    }
  }

  public render() {
    const {isDragging, topPercent, bottomPercent} = this.state
    const {children, topMinPixels, bottomMinPixels, orientation} = this.props

    if (React.Children.count(children) !== REQUIRED_HALVES) {
      console.error('ResizeContainer requires exactly 2 children')
      return null
    }

    return (
      <div
        className={this.className}
        onMouseLeave={this.handleMouseLeave}
        onMouseUp={this.handleStopDrag}
        onMouseMove={this.handleDrag}
        ref={r => (this.containerRef = r)}
      >
        <ResizeHalf
          offset={0}
          percent={topPercent}
          minPixels={topMinPixels}
          component={children[0]}
          orientation={orientation}
        />
        <ResizeHandle
          onStartDrag={this.handleStartDrag}
          percent={topPercent}
          isDragging={isDragging}
          orientation={orientation}
        />
        <ResizeHalf
          offset={topPercent}
          percent={bottomPercent}
          minPixels={bottomMinPixels}
          component={children[1]}
          orientation={orientation}
        />
      </div>
    )
  }

  private get className(): string {
    const {orientation, containerClass} = this.props
    const {isDragging} = this.state

    return classnames(`resize--container ${containerClass}`, {
      dragging: isDragging,
      horizontal: orientation === HANDLE_HORIZONTAL,
      vertical: orientation === HANDLE_VERTICAL,
    })
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

  private handleDrag = (e: MouseEvent<HTMLElement>) => {
    const {isDragging} = this.state
    const {orientation} = this.props
    if (!isDragging) {
      return
    }

    const {percentX, percentY} = this.mousePosWithinContainer(e)

    if (orientation === HANDLE_HORIZONTAL && this.dragIsWithinBounds(e)) {
      this.setState({
        topPercent: percentY,
        bottomPercent: this.invertPercent(percentY),
      })
    }

    if (orientation === HANDLE_VERTICAL && this.dragIsWithinBounds(e)) {
      this.setState({
        topPercent: percentX,
        bottomPercent: this.invertPercent(percentX),
      })
    }
  }

  private dragIsWithinBounds = (e: MouseEvent<HTMLElement>): boolean => {
    const {orientation, topMinPixels, bottomMinPixels} = this.props
    const {mouseX, mouseY} = this.mousePosWithinContainer(e)
    const {width, height} = this.containerRef.getBoundingClientRect()

    if (orientation === HANDLE_HORIZONTAL) {
      const doesNotExceedTop = mouseY > topMinPixels
      const doesNotExceedBottom = Math.abs(mouseY - height) > bottomMinPixels

      return doesNotExceedTop && doesNotExceedBottom
    }

    const doesNotExceedLeft = mouseX > topMinPixels
    const doesNotExceedRight = Math.abs(mouseX - width) > bottomMinPixels

    return doesNotExceedLeft && doesNotExceedRight
  }

  private mousePosWithinContainer = (e: MouseEvent<HTMLElement>) => {
    const {pageY, pageX} = e
    const {top, left, width, height} = this.containerRef.getBoundingClientRect()

    const mouseX = pageX - left
    const mouseY = pageY - top

    const percentX = mouseX / width
    const percentY = mouseY / height

    return {
      mouseX,
      mouseY,
      percentX,
      percentY,
    }
  }

  private invertPercent = percent => {
    return 1 - percent
  }
}

export default Resizer
