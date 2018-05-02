import React, {Component, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'
import uuid from 'uuid'
import _ from 'lodash'

import ResizeHalf from 'src/shared/components/ResizeHalf'
import ResizeHandle from 'src/shared/components/ResizeHandle'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_HORIZONTAL, HANDLE_VERTICAL} from 'src/shared/constants/index'

interface State {
  topPercent: number
  bottomPercent: number
  isDragging: boolean
}

interface Props {
  topHalf: () => ReactElement<any>
  topMinPixels: number
  bottomHalf: () => ReactElement<any>
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
  private percentChangeX: number = 0
  private percentChangeY: number = 0

  constructor(props) {
    super(props)
    this.state = {
      topPercent: 0.5,
      bottomPercent: 0.5,
      isDragging: false,
    }
  }

  public componentDidUpdate(__, prevState) {
    const {dragEvent} = this.state
    const {orientation} = this.props

    if (_.isEqual(dragEvent, prevState.dragEvent)) {
      return
    }

    this.percentChangeX = this.pixelsToPercentX(
      prevState.dragEvent.mouseX,
      dragEvent.mouseX
    )

    this.percentChangeY = this.pixelsToPercentY(
      prevState.dragEvent.mouseY,
      dragEvent.mouseY
    )

    if (orientation === HANDLE_VERTICAL) {
      const left = dragEvent.percentX < prevState.dragEvent.percentX

      if (left) {
        return this.move.left()
      }

      return this.move.right()
    }

    const up = dragEvent.percentY < prevState.dragEvent.percentY
    const down = dragEvent.percentY > prevState.dragEvent.percentY

    if (up) {
      return this.move.up()
    } else if (down) {
      return this.move.down()
    }
  }

  public render() {
    const {isDragging, topPercent, bottomPercent} = this.state
    const {
      topHalf,
      topMinPixels,
      bottomHalf,
      bottomMinPixels,
      orientation,
    } = this.props

    return (
      <div
        className={this.className}
        onMouseLeave={this.handleMouseLeave}
        onMouseUp={this.handleStopDrag}
        onMouseMove={this.handleDrag}
        ref={r => (this.containerRef = r)}
      >
        <ResizeHalf
          percent={topPercent}
          minPixels={topMinPixels}
          render={topHalf}
          orientation={orientation}
        />
        <ResizeHandle
          onStartDrag={this.handleStartDrag}
          isDragging={isDragging}
          orientation={orientation}
        />
        <ResizeHalf
          percent={bottomPercent}
          minPixels={bottomMinPixels}
          render={bottomHalf}
          orientation={orientation}
        />
      </div>
    )
  }

  private minPercent = (minPixels: number): number => {
    if (this.props.orientation === HANDLE_VERTICAL) {
      return this.minPercentX(minPixels)
    }

    return this.minPercentY(minPixels)
  }

  private get className(): string {
    const {orientation, containerClass} = this.props

    return classnames(`resize--container ${containerClass}`, {
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

  private pixelsToPercentX = (startValue, endValue) => {
    if (!startValue) {
      return 0
    }

    const delta = startValue - endValue
    const {width} = this.containerRef.getBoundingClientRect()

    return Math.abs(delta / width)
  }

  private pixelsToPercentY = (startValue, endValue) => {
    if (!startValue) {
      return 0
    }

    const delta = startValue - endValue
    const {height} = this.containerRef.getBoundingClientRect()

    return Math.abs(delta / height)
  }

  private minPercentX = (xMinPixels: number): number => {
    if (!this.containerRef) {
      return 0
    }
    const {width} = this.containerRef.getBoundingClientRect()

    return xMinPixels / width
  }

  private minPercentY = (yMinPixels: number): number => {
    if (!this.containerRef) {
      return 0
    }

    const {height} = this.containerRef.getBoundingClientRect()
    return yMinPixels / height
  }

  private handleDrag = (e: MouseEvent<HTMLElement>) => {
    const {activeHandleID} = this.state
    if (!activeHandleID) {
      return
    }

    const dragEvent = this.mousePosWithinContainer(e)
    this.setState({dragEvent})
  }

  private taller = (size: number): number => {
    const newSize = size + this.percentChangeY
    return Number(newSize.toFixed(3))
  }

  private shorter = (size: number): number => {
    const newSize = size - this.percentChangeY
    return Number(newSize.toFixed(3))
  }

  private isAtMinHeight = (division: DivisionState): boolean => {
    return division.size <= this.minPercentY(division.minPixels)
  }
}

export default Resizer
