import React, {Component, ReactElement} from 'react'
import classnames from 'classnames'
import uuid from 'uuid'

import ResizeDivision from 'src/shared/components/ResizeDivision'
import {ErrorHandling} from 'src/shared/decorators/errors'
import {
  MIN_DIVISIONS,
  ORIENTATION_HORIZONTAL,
  ORIENTATION_VERTICAL,
} from 'src/shared/constants/'

interface State {
  activeHandleID: string
  divisions: DivisionState[]
}

interface Division {
  name?: string
  minSize?: number
  render: () => ReactElement<any>
}

interface DivisionState extends Division {
  id: string
  size: number
}

interface Props {
  divisions: Division[]
  orientation: string
  containerClass: string
}

@ErrorHandling
class Resizer extends Component<Props, State> {
  public static defaultProps: Partial<Props> = {
    orientation: ORIENTATION_HORIZONTAL,
  }

  public containerRef: HTMLElement

  constructor(props) {
    super(props)
    this.state = {
      activeHandleID: null,
      divisions: this.initialDivisions,
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
            render={d.render}
            orientation={orientation}
            draggable={i > 0}
            activeHandleID={activeHandleID}
            onHandleStartDrag={this.handleStartDrag}
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
      horizontal: orientation === ORIENTATION_HORIZONTAL,
      vertical: orientation === ORIENTATION_VERTICAL,
    })
  }

  private get initialDivisions() {
    const {divisions} = this.props

    const size = 1 / divisions.length

    return divisions.map(d => ({
      ...d,
      id: uuid.v4(),
      size,
    }))
  }

  private handleStartDrag = activeHandleID => {
    this.setState({activeHandleID})
  }

  private handleStopDrag = () => {
    this.setState({activeHandleID: ''})
  }

  private handleMouseLeave = () => {
    this.setState({activeHandleID: ''})
  }

  private handleDrag = () => {
    // const {divisions, activeHandleID} = this.state
    // if (!this.state.activeHandleID) {
    //   return
    // }
    // const activeDivision = divisions.find(d => d.id === activeHandleID)
    // if (!activeDivision) {
    //   return
    // }
    // const {size, offset} = activeDivision
    // const {height} = getComputedStyle(this.containerRef)
    // const containerHeight = parseInt(height, 10)
    // // verticalOffset moves the resize handle as many pixels as the page-heading is taking up.
    // const verticalOffset = window.innerHeight - containerHeight
    // const newTopPanelPercent = Math.ceil(
    //   (e.pageY - verticalOffset) / containerHeight * HUNDRED
    // )
    // const newBottomPanelPercent = HUNDRED - newTopPanelPercent
    // // Don't trigger a resize unless the change in size is greater than minResizePercentage
    // const minResizePercentage = 0.5
    // if (
    //   Math.abs(newTopPanelPercent - this.state.topHeight) < minResizePercentage
    // ) {
    //   return
    // }
    // const topHeightPixels = newTopPanelPercent / HUNDRED * containerHeight
    // const bottomHeightPixels = newBottomPanelPercent / HUNDRED * containerHeight
    // // Don't trigger a resize if the new sizes are too small
    // if (
    //   topHeightPixels < minTopHeight ||
    //   bottomHeightPixels < minBottomHeight
    // ) {
    //   return
    // }
    // this.setState({
    //   topHeight: newTopPanelPercent,
    //   topHeightPixels,
    //   bottomHeight: newBottomPanelPercent,
    //   bottomHeightPixels,
    // })
  }
}

export default Resizer
