import React, {Component, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'
import uuid from 'uuid'
import _ from 'lodash'

import ResizeDivision from 'src/shared/components/ResizeDivision'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  HANDLE_PIXELS,
  HANDLE_HORIZONTAL,
  HANDLE_VERTICAL,
} from 'src/shared/constants/'

const initialDragEvent = {
  percentX: 0,
  percentY: 0,
  mouseX: null,
  mouseY: null,
}

interface State {
  activeHandleID: string
  divisions: DivisionState[]
  dragDirection: string
  dragEvent: any
}

interface Division {
  name?: string
  render: () => ReactElement<any>
  minPixels?: number
}

interface DivisionState extends Division {
  id: string
  size: number
  minPixels?: number
}

interface Props {
  divisions: Division[]
  orientation: string
  containerClass?: string
}

@ErrorHandling
class Threesizer extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    orientation: HANDLE_HORIZONTAL,
  }

  private containerRef: HTMLElement
  private percentChangeX: number = 0
  private percentChangeY: number = 0

  constructor(props) {
    super(props)
    this.state = {
      activeHandleID: null,
      divisions: this.initialDivisions,
      dragEvent: initialDragEvent,
      dragDirection: '',
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
    const {activeHandleID, divisions} = this.state
    const {orientation} = this.props

    return (
      <div
        className={this.className}
        onMouseLeave={this.handleMouseLeave}
        onMouseUp={this.handleStopDrag}
        onMouseMove={this.handleDrag}
        ref={r => (this.containerRef = r)}
      >
        {divisions.map((d, i) => (
          <ResizeDivision
            key={d.id}
            id={d.id}
            name={d.name}
            size={d.size}
            offset={this.offset}
            draggable={i > 0}
            minPixels={d.minPixels}
            orientation={orientation}
            activeHandleID={activeHandleID}
            onDoubleClick={this.handleDoubleClick}
            onHandleStartDrag={this.handleStartDrag}
            render={this.props.divisions[i].render}
          />
        ))}
      </div>
    )
  }

  private get offset(): number {
    return HANDLE_PIXELS * this.state.divisions.length
  }

  private get className(): string {
    const {orientation, containerClass} = this.props
    const {activeHandleID} = this.state

    return classnames(`threesizer ${containerClass}`, {
      dragging: activeHandleID,
      horizontal: orientation === HANDLE_HORIZONTAL,
      vertical: orientation === HANDLE_VERTICAL,
    })
  }

  private get initialDivisions() {
    const {divisions} = this.props

    const size = 1 / divisions.length

    return divisions.map(d => ({
      ...d,
      id: uuid.v4(),
      size,
      minPixels: d.minPixels || 0,
    }))
  }

  private handleDoubleClick = (id: string): void => {
    const clickedDiv = this.state.divisions.find(d => d.id === id)

    if (!clickedDiv) {
      return
    }

    const isMaxed = clickedDiv.size === 1

    if (isMaxed) {
      return this.equalize()
    }

    const divisions = this.state.divisions.map(d => {
      if (d.id !== id) {
        return {...d, size: 0}
      }

      return {...d, size: 1}
    })

    this.setState({divisions})
  }

  private equalize = () => {
    const denominator = this.state.divisions.length
    const divisions = this.state.divisions.map(d => {
      return {...d, size: 1 / denominator}
    })

    this.setState({divisions})
  }

  private handleStartDrag = (activeHandleID, e: MouseEvent<HTMLElement>) => {
    const dragEvent = this.mousePosWithinContainer(e)
    this.setState({activeHandleID, dragEvent})
  }

  private handleStopDrag = () => {
    this.setState({activeHandleID: '', dragEvent: initialDragEvent})
  }

  private handleMouseLeave = () => {
    this.setState({activeHandleID: '', dragEvent: initialDragEvent})
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

  private handleDrag = (e: MouseEvent<HTMLElement>) => {
    const {activeHandleID} = this.state
    if (!activeHandleID) {
      return
    }

    const dragEvent = this.mousePosWithinContainer(e)
    this.setState({dragEvent})
  }

  private get move() {
    const {activeHandleID} = this.state

    const activePosition = _.findIndex(
      this.state.divisions,
      d => d.id === activeHandleID
    )

    return {
      up: this.up(activePosition),
      down: this.down(activePosition),
      left: this.left(activePosition),
      right: this.right(activePosition),
    }
  }

  private up = activePosition => () => {
    const divisions = this.state.divisions.map((d, i) => {
      if (!activePosition) {
        return d
      }

      const first = i === 0
      const before = i === activePosition - 1
      const current = i === activePosition

      if (first && !before) {
        const second = this.state.divisions[1]
        if (second.size === 0) {
          return {...d, size: this.shorter(d.size)}
        }

        return {...d}
      }

      if (before) {
        return {...d, size: this.shorter(d.size)}
      }

      if (current) {
        return {...d, size: this.taller(d.size)}
      }

      return {...d}
    })

    this.setState({divisions})
  }

  private down = activePosition => () => {
    const divisions = this.state.divisions.map((d, i, divs) => {
      const before = i === activePosition - 1
      const current = i === activePosition
      const after = i === activePosition + 1

      if (before) {
        return {...d, size: this.taller(d.size)}
      }

      if (current) {
        return {...d, size: this.shorter(d.size)}
      }

      if (after) {
        const above = divs[i - 1]
        if (above.size === 0) {
          return {...d, size: this.shorter(d.size)}
        }

        return {...d}
      }

      return {...d}
    })

    this.setState({divisions})
  }

  private left = activePosition => () => {
    const divisions = this.state.divisions.map((d, i) => {
      const before = i === activePosition - 1
      const active = i === activePosition

      if (before) {
        return {...d, size: d.size - this.percentChangeX}
      } else if (active) {
        return {...d, size: d.size + this.percentChangeX}
      }

      return d
    })

    this.setState({divisions})
  }

  private right = activePosition => () => {
    const divisions = this.state.divisions.map((d, i) => {
      const before = i === activePosition - 1
      const active = i === activePosition

      if (before) {
        return {...d, size: d.size + this.percentChangeX}
      } else if (active) {
        return {...d, size: d.size - this.percentChangeX}
      }

      return d
    })

    this.setState({divisions})
  }

  private taller = (size: number): number => {
    const newSize = size + this.percentChangeY
    return newSize > 1 ? 1 : newSize
  }

  private shorter = (size: number): number => {
    const newSize = size - this.percentChangeY
    return newSize < 0 ? 0 : newSize
  }
}

export default Threesizer
