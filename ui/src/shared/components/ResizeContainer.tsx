import React, {Component, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'
import uuid from 'uuid'
import _ from 'lodash'

import ResizeDivision from 'src/shared/components/ResizeDivision'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  MIN_DIVISIONS,
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

    if (divisions.length < MIN_DIVISIONS) {
      console.error(
        `There must be at least ${MIN_DIVISIONS}' divisions in Resizer`
      )
      return
    }

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
            draggable={i > 0}
            minPixels={d.minPixels}
            orientation={orientation}
            activeHandleID={activeHandleID}
            onHandleStartDrag={this.handleStartDrag}
            maxPercent={this.maximumHeightPercent}
            render={this.props.divisions[i].render}
          />
        ))}
      </div>
    )
  }

  private get className(): string {
    const {orientation, containerClass} = this.props
    const {activeHandleID} = this.state

    return classnames(`resize--container ${containerClass}`, {
      'resize--dragging': activeHandleID,
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

  // private minPercentX = (xMinPixels: number): number => {
  //   const {height} = this.containerRef.getBoundingClientRect()

  //   return xMinPixels / height
  // }

  private minPercentY = (yMinPixels: number): number => {
    if (!this.containerRef) {
      return 0
    }

    const {height} = this.containerRef.getBoundingClientRect()
    return yMinPixels / height
  }

  private get maximumHeightPercent(): number {
    if (!this.containerRef) {
      return 1
    }

    const {divisions} = this.state
    const {height} = this.containerRef.getBoundingClientRect()

    const totalMinPixels = divisions.reduce(
      (acc, div) => acc + div.minPixels,
      0
    )

    const maximumPixels = height - totalMinPixels

    return this.minPercentY(maximumPixels)
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
    const divisions = this.state.divisions.map((d, i, divs) => {
      const before = i < activePosition
      const current = i === activePosition
      const after = i === activePosition + 1

      if (before) {
        const below = divs[i + 1]
        const belowAtMinimum = below.size <= this.minPercentY(below.minPixels)

        const aboveCurrent = i === activePosition - 1

        if (belowAtMinimum || aboveCurrent) {
          const size = d.size - this.percentChangeY
          return {...d, size}
        }
      }

      if (current) {
        return {...d, size: d.size + this.percentChangeY}
      }

      if (after) {
        return {...d}
      }

      return {...d}
    })

    this.setState({divisions})
  }

  private down = activePosition => () => {
    const divisions = this.state.divisions.map((d, i, divs) => {
      const before = i === activePosition - 1
      const current = i === activePosition
      const after = i > activePosition

      if (before) {
        const size = d.size + this.percentChangeY
        return {...d, size}
      }

      if (current) {
        const size = d.size - this.percentChangeY

        return {...d, size}
      }

      if (after) {
        const previous = divs[i - 1]
        const prevAtMinimum =
          previous.size <= this.minPercentY(previous.minPixels)

        if (prevAtMinimum) {
          const size = d.size - this.percentChangeY
          return {...d, size}
        }
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

  private enforceHundredTotal = divisions => {
    const indexLast = divisions.length - 1
    const subTotal = divisions
      .slice(0, indexLast)
      .reduce((acc, div) => acc + div.size, 0)

    divisions[indexLast].size = 1 - subTotal

    return divisions
  }
}

export default Resizer
