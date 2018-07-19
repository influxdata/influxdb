import React, {
  PureComponent,
  RefObject,
  MouseEvent as ReactMouseEvent,
} from 'react'
import {ScaleTime} from 'd3-scale'

import {TimePeriod} from 'src/types/histogram'

interface Props {
  width: number
  height: number
  xScale: ScaleTime<number, number>
  onBrush?: (t: TimePeriod) => void
  onDoubleClick?: () => void
}

interface State {
  dragging: boolean
  dragStartPos: number
  dragPos: number
}

class XBrush extends PureComponent<Props, State> {
  private draggableArea: RefObject<SVGRectElement>

  constructor(props) {
    super(props)

    this.state = {
      dragging: false,
      dragStartPos: 0,
      dragPos: 0,
    }

    this.draggableArea = React.createRef<SVGRectElement>()
  }

  public componentWillUnmount() {
    // These are usually cleaned up on handleDragEnd; this ensures they will
    // also be cleaned up if the component is destroyed mid-brush
    document.removeEventListener('movemove', this.handleDrag)
    document.removeEventListener('mouseup', this.handleDragEnd)
  }

  public render() {
    const {width, height} = this.props

    return (
      <>
        {this.renderSelection()}
        <rect
          ref={this.draggableArea}
          className="x-brush--area"
          width={width}
          height={height}
          onMouseDown={this.handleDragStart}
          onDoubleClick={this.handleDoubleClick}
        />
      </>
    )
  }

  private renderSelection(): JSX.Element {
    const {height} = this.props
    const {dragging, dragStartPos, dragPos} = this.state

    if (!dragging) {
      return null
    }

    const x = Math.min(dragStartPos, dragPos)
    const width = Math.abs(dragStartPos - dragPos)

    return (
      <rect
        className="x-brush--selection"
        y={0}
        height={height}
        x={x}
        width={width}
      />
    )
  }

  private handleDragStart = (e: ReactMouseEvent<SVGRectElement>): void => {
    // A user can mousedown (start a brush) then move outside of the current
    // element while still holding the mouse down, therfore we must listen to
    // mouse events everywhere, not just within this component.
    document.addEventListener('mousemove', this.handleDrag)
    document.addEventListener('mouseup', this.handleDragEnd)

    const x = this.getX(e)

    this.setState({
      dragging: true,
      dragStartPos: x,
      dragPos: x,
    })
  }

  private handleDrag = (e: MouseEvent): void => {
    const {dragging} = this.state

    if (!dragging) {
      return
    }

    this.setState({dragPos: this.getX(e)})
  }

  private handleDragEnd = (): void => {
    document.removeEventListener('movemove', this.handleDrag)
    document.removeEventListener('mouseup', this.handleDragEnd)

    const {xScale, onBrush} = this.props
    const {dragging, dragPos, dragStartPos} = this.state

    if (!dragging) {
      return
    }

    this.setState({dragging: false})

    if (!onBrush || Math.round(dragPos) === Math.round(dragStartPos)) {
      return
    }

    const startX = Math.min(dragStartPos, dragPos)
    const endX = Math.max(dragStartPos, dragPos)
    const start = xScale.invert(startX).getTime()
    const end = xScale.invert(endX).getTime()

    onBrush({start, end})
  }

  private handleDoubleClick = (): void => {
    const {onDoubleClick} = this.props

    if (onDoubleClick) {
      onDoubleClick()
    }
  }

  private getX = (e: MouseEvent | ReactMouseEvent<SVGRectElement>): number => {
    const {width} = this.props

    const {left} = this.draggableArea.current.getBoundingClientRect()
    const x = e.pageX - left

    if (x < 0) {
      return 0
    }

    if (x > width) {
      return width
    }

    return x
  }
}

export default XBrush
