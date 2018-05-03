import React, {PureComponent, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'

const NOOP = () => {}

interface Props {
  id: string
  name?: string
  minPixels: number
  size: number
  offset: number
  activeHandleID: string
  draggable: boolean
  orientation: string
  render: () => ReactElement<any>
  onHandleStartDrag: (id: string, e: MouseEvent<HTMLElement>) => void
  onDoubleClick: (id: string) => void
}

class Division extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    name: '',
  }

  public render() {
    const {name, render} = this.props
    return (
      <>
        <div className={this.containerClass} style={this.containerStyle}>
          <div
            draggable={true}
            className={this.className}
            onDragStart={this.drag}
            onDoubleClick={this.handleDoubleClick}
            title={this.title}
          >
            {name}
          </div>
          <FancyScrollbar className="threesizer--contents" autoHide={true}>
            {render()}
          </FancyScrollbar>
        </div>
      </>
    )
  }

  private get title() {
    return 'Drag to resize.\nDouble click to expand.'
  }

  private get containerStyle() {
    const {size, offset} = this.props

    return {
      height: `calc((100% - ${offset}px) * ${size} + 30px)`,
    }
  }

  private get containerClass(): string {
    const isAnyHandleBeingDragged = !!this.props.activeHandleID
    return classnames('threesizer--division', {
      dragging: isAnyHandleBeingDragged,
    })
  }

  private get className(): string {
    const {draggable} = this.props

    return classnames('threesizer--handle', {
      disabled: !draggable,
      dragging: this.isDragging,
    })
  }

  private get isDragging(): boolean {
    const {id, activeHandleID} = this.props
    return id === activeHandleID
  }

  private drag = e => {
    const {draggable, id} = this.props

    if (!draggable) {
      return NOOP
    }

    this.props.onHandleStartDrag(id, e)
  }

  private handleDoubleClick = () => {
    const {onDoubleClick, id} = this.props

    onDoubleClick(id)
  }
}

export default Division
