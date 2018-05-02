import React, {Component, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'
import uuid from 'uuid'
import _ from 'lodash'

import ResizeDivision from 'src/shared/components/ResizeDivision'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {HANDLE_HORIZONTAL, HANDLE_VERTICAL} from 'src/shared/constants/'

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

  private minPercent = (minPixels: number): number => {
    if (this.props.orientation === HANDLE_VERTICAL) {
      return this.minPercentX(minPixels)
    }

    return this.minPercentY(minPixels)
  }

  private get className(): string {
    const {orientation} = this.props
    const {activeHandleID} = this.state

    return classnames(`threesizer`, {
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

  private taller = (size: number): number => {
    const newSize = size + this.percentChangeY
    return newSize > 1 ? 1 : newSize
  }

  private shorter = (size: number): number => {
    const newSize = size - this.percentChangeY
    return newSize < 0 ? 0 : newSize
  }

  private isAtMinHeight = (division: DivisionState): boolean => {
    return division.size <= this.minPercentY(division.minPixels)
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
      const first = i === 0
      const before = i === activePosition - 1
      const current = i === activePosition

      if (first) {
        const below = divs[i + 1]
        if (below.size === 0) {
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

  private enforceSize = (size, minPixels): number => {
    const minPercent = this.minPercent(minPixels)

    let enforcedSize = size
    if (size < minPercent) {
      enforcedSize = minPercent
    }

    return enforcedSize
  }

  private cleanDivisions = divisions => {
    const minSizes = divisions.map(d => {
      const size = this.enforceSize(d.size, d.minPixels)
      return {...d, size}
    })

    const sumSizes = minSizes.reduce((acc, div, i) => {
      if (i <= divisions.length - 1) {
        return acc + div.size
      }

      return acc
    }, 0)

    const under100percent = 1 - sumSizes > 0
    const over100percent = 1 - sumSizes < 0

    if (under100percent) {
      minSizes[divisions.length - 1].size += Math.abs(1 - sumSizes)
    }

    if (over100percent) {
      minSizes[divisions.length - 1].size -= Math.abs(1 - sumSizes)
    }

    return minSizes
  }
}

export default Resizer
