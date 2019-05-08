// Libraries
import React, {Component, RefObject} from 'react'
import classnames from 'classnames'

// Components
import DraggableResizerPanel from 'src/clockface/components/draggable_resizer/DraggableResizerPanel'
import DraggableResizerHandle from 'src/clockface/components/draggable_resizer/DraggableResizerHandle'
import {Stack} from 'src/clockface/types'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[]
  stackPanels: Stack
  handlePositions: number[]
  onChangePositions: (positions: number[]) => void
}

interface State {
  initialized: boolean
  dragIndex: number | null
}

@ErrorHandling
class DraggableResizer extends Component<Props, State> {
  public static Panel = DraggableResizerPanel

  private containerRef: RefObject<HTMLDivElement>

  constructor(props: Props) {
    super(props)

    this.containerRef = React.createRef()

    this.state = {
      initialized: false,
      dragIndex: null,
    }
  }

  public componentDidMount() {
    // This allows for one extra render which
    // ensures the panels don't render without positioning information
    this.setState({initialized: true})
  }

  public render() {
    return (
      <div className={this.className} ref={this.containerRef}>
        {this.children}
      </div>
    )
  }

  private get className(): string {
    const {stackPanels} = this.props
    const {dragIndex} = this.state

    const isDragging = typeof dragIndex === 'number'

    return classnames('draggable-resizer', {
      'draggable-resizer--vertical': stackPanels === Stack.Columns,
      'draggable-resizer--horizontal': stackPanels === Stack.Rows,
      'draggable-resizer--dragging': isDragging,
    })
  }

  private get children(): JSX.Element[] {
    const {children} = this.props
    const {dragIndex, initialized} = this.state

    const panelsCount = React.Children.count(children)

    if (initialized) {
      return React.Children.map(children, (child: JSX.Element, i: number) => {
        if (child.type !== DraggableResizerPanel) {
          return
        }

        const isLastPanel = i === panelsCount - 1
        const dragging = i === dragIndex

        return (
          <>
            <DraggableResizerPanel
              {...child.props}
              sizePercent={this.calculatePanelSize(i)}
            />
            {!isLastPanel && (
              <DraggableResizerHandle
                {...child.props}
                dragIndex={i}
                onStartDrag={this.handleStartDrag}
                dragging={dragging}
              />
            )}
          </>
        )
      })
    }
  }

  private handleStartDrag = (dragIndex: number): void => {
    this.setState({dragIndex})

    window.addEventListener('mousemove', this.handleDrag)
    window.addEventListener('mouseup', this.handleStopDrag)
  }

  private handleStopDrag = (): void => {
    this.setState({dragIndex: null})

    window.removeEventListener('mousemove', this.handleDrag)
    window.removeEventListener('mouseup', this.handleStopDrag)
  }

  private handleDrag = (e: MouseEvent): void => {
    const {handlePositions, onChangePositions, stackPanels} = this.props
    const {dragIndex} = this.state
    const {
      x,
      y,
      width,
      height,
    } = this.containerRef.current.getBoundingClientRect() as DOMRect

    let containerSize = width
    // The single-axis position of the mouse relative to the `.draggable-resizer` container
    let mouseRelativePos = e.pageX - x

    // Use correct properties in case of horizontality
    if (stackPanels === Stack.Rows) {
      containerSize = height
      mouseRelativePos = e.pageY - y
    }

    // Clamp `mouseRelativePos` to the container
    if (mouseRelativePos < 0) {
      mouseRelativePos = 0
    } else if (mouseRelativePos > containerSize) {
      mouseRelativePos = containerSize
    }

    const newPos = mouseRelativePos / containerSize
    const newPositions = [...handlePositions]

    // Update the position of the handle being dragged
    newPositions[dragIndex] = newPos

    // If the new position of the handle being dragged is greater than
    // subsequent handles on the right, set them all to the new position to
    // acheive the collapsing / “accordian” effect
    for (let i = dragIndex + 1; i < newPositions.length; i++) {
      if (newPos > newPositions[i]) {
        newPositions[i] = newPos
      }
    }

    // Do something similar for handles on the left
    for (let i = 0; i < dragIndex; i++) {
      if (newPos < newPositions[i]) {
        newPositions[i] = newPos
      }
    }

    onChangePositions(newPositions)
  }

  private calculatePanelSize = (panelIndex: number): number => {
    const {handlePositions} = this.props
    const prevPanelIndex = panelIndex - 1

    if (panelIndex === 0) {
      return handlePositions[panelIndex]
    }

    if (panelIndex === handlePositions.length) {
      return 1 - handlePositions[prevPanelIndex]
    }

    return handlePositions[panelIndex] - handlePositions[prevPanelIndex]
  }
}

export default DraggableResizer
