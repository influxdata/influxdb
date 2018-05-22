import React, {Component, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'
import uuid from 'uuid'
import _ from 'lodash'

import Division from 'src/shared/components/threesizer/Division'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {MenuItem} from 'src/shared/components/threesizer/DivisionMenu'

import {
  HANDLE_NONE,
  HANDLE_PIXELS,
  HANDLE_HORIZONTAL,
  HANDLE_VERTICAL,
  MIN_SIZE,
  MAX_SIZE,
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

interface DivisionProps {
  name?: string
  handleDisplay?: string
  handlePixels?: number
  headerButtons?: JSX.Element[]
  menuOptions: MenuItem[]
  render: (visibility?: string) => ReactElement<any>
}

interface DivisionState extends DivisionProps {
  id: string
  size: number
}

interface Props {
  divisions: DivisionProps[]
  orientation: string
  containerClass?: string
}

@ErrorHandling
class Threesizer extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    orientation: HANDLE_HORIZONTAL,
    containerClass: '',
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

  public componentDidMount() {
    document.addEventListener('mouseup', this.handleStopDrag)
    document.addEventListener('mouseleave', this.handleStopDrag)
  }

  public componentWillUnmount() {
    document.removeEventListener('mouseup', this.handleStopDrag)
    document.removeEventListener('mouseleave', this.handleStopDrag)
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

    const {percentX, percentY} = dragEvent
    const {dragEvent: prevDrag} = prevState

    if (orientation === HANDLE_VERTICAL) {
      const left = percentX < prevDrag.percentX

      if (left) {
        return this.move.left()
      }

      return this.move.right()
    }

    const up = percentY < prevDrag.percentY

    if (up) {
      return this.move.up()
    }

    return this.move.down()
  }

  public render() {
    const {activeHandleID, divisions} = this.state
    const {orientation} = this.props

    return (
      <div
        className={this.className}
        onMouseUp={this.handleStopDrag}
        onMouseMove={this.handleDrag}
        ref={r => (this.containerRef = r)}
      >
        {divisions.map((d, i) => (
          <Division
            key={d.id}
            id={d.id}
            name={d.name}
            size={d.size}
            offset={this.offset}
            draggable={i > 0}
            orientation={orientation}
            handlePixels={d.handlePixels}
            handleDisplay={d.handleDisplay}
            activeHandleID={activeHandleID}
            onMaximize={this.handleMaximize}
            onMinimize={this.handleMinimize}
            onDoubleClick={this.handleDoubleClick}
            render={this.props.divisions[i].render}
            onHandleStartDrag={this.handleStartDrag}
            menuOptions={this.props.divisions[i].menuOptions}
            headerButtons={this.props.divisions[i].headerButtons}
          />
        ))}
      </div>
    )
  }

  private get offset(): number {
    const handlesPixelCount = this.state.divisions.reduce((acc, d) => {
      if (d.handleDisplay === HANDLE_NONE) {
        return acc
      }

      return acc + d.handlePixels
    }, 0)

    return handlesPixelCount
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
      handlePixels: d.handlePixels || HANDLE_PIXELS,
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

  private handleMaximize = (id: string): void => {
    const maxDiv = this.state.divisions.find(d => d.id === id)

    if (!maxDiv) {
      return
    }

    const divisions = this.state.divisions.map(d => {
      if (d.id !== id) {
        return {...d, size: 0}
      }

      return {...d, size: 1}
    })

    this.setState({divisions})
  }

  private handleMinimize = (id: string): void => {
    const minDiv = this.state.divisions.find(d => d.id === id)
    const numDivisions = this.state.divisions.length

    if (!minDiv) {
      return
    }

    let size
    if (numDivisions <= 1) {
      size = 1
    } else {
      size = 1 / (this.state.divisions.length - 1)
    }

    const divisions = this.state.divisions.map(d => {
      if (d.id !== id) {
        return {...d, size}
      }

      return {...d, size: 0}
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
    if (!startValue || !endValue) {
      return 0
    }

    const delta = Math.abs(startValue - endValue)
    const {width} = this.containerRef.getBoundingClientRect()

    return delta / width
  }

  private pixelsToPercentY = (startValue, endValue) => {
    if (!startValue || !endValue) {
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
        if (second && second.size === 0) {
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

  private left = activePosition => () => {
    const divisions = this.state.divisions.map((d, i) => {
      if (!activePosition) {
        return d
      }

      const first = i === 0
      const before = i === activePosition - 1
      const active = i === activePosition

      if (first && !before) {
        const second = this.state.divisions[1]
        if (second && second.size === 0) {
          return {...d, size: this.thinner(d.size)}
        }

        return {...d}
      }

      if (before) {
        return {...d, size: this.thinner(d.size)}
      }

      if (active) {
        return {...d, size: this.fatter(d.size)}
      }

      return {...d}
    })

    this.setState({divisions})
  }

  private right = activePosition => () => {
    const divisions = this.state.divisions.map((d, i, divs) => {
      const before = i === activePosition - 1
      const active = i === activePosition
      const after = i === activePosition + 1

      if (before) {
        return {...d, size: this.fatter(d.size)}
      }

      if (active) {
        return {...d, size: this.thinner(d.size)}
      }

      if (after) {
        const leftIndex = i - 1
        const left = _.get(divs, leftIndex, {size: 'none'})

        if (left && left.size === 0) {
          return {...d, size: this.thinner(d.size)}
        }

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
      const after = i === activePosition + 1

      if (before) {
        return {...d, size: this.taller(d.size)}
      }

      if (current) {
        return {...d, size: this.shorter(d.size)}
      }

      if (after) {
        const above = divs[i - 1]
        if (above && above.size === 0) {
          return {...d, size: this.shorter(d.size)}
        }

        return {...d}
      }

      return {...d}
    })

    this.setState({divisions})
  }

  private taller = (size: number): number => {
    const newSize = size + this.percentChangeY
    return this.enforceMax(newSize)
  }

  private fatter = (size: number): number => {
    const newSize = size + this.percentChangeX
    return this.enforceMax(newSize)
  }

  private shorter = (size: number): number => {
    const newSize = size - this.percentChangeY
    return this.enforceMin(newSize)
  }

  private thinner = (size: number): number => {
    const newSize = size - this.percentChangeX
    return this.enforceMin(newSize)
  }

  private enforceMax = (size: number): number => {
    return size > MAX_SIZE ? MAX_SIZE : size
  }

  private enforceMin = (size: number): number => {
    return size < MIN_SIZE ? MIN_SIZE : size
  }
}

export default Threesizer
