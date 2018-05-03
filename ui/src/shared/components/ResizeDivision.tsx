import React, {PureComponent, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'

import {
  HANDLE_PIXELS,
  HANDLE_VERTICAL,
  HANDLE_HORIZONTAL,
} from 'src/shared/constants/index'

const NOOP = () => {}

interface Props {
  name?: string
  handleDisplay?: string
  id: string
  size: number
  offset: number
  draggable: boolean
  orientation: string
  activeHandleID: string
  render: () => ReactElement<any>
  onHandleStartDrag: (id: string, e: MouseEvent<HTMLElement>) => void
  onDoubleClick: (id: string) => void
}

class Division extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    name: '',
    handleDisplay: 'visible',
  }

  public render() {
    const {name, render, orientation, draggable} = this.props
    return (
      <>
        <div className={this.containerClass} style={this.containerStyle}>
          <div
            style={this.handleStyle}
            title={this.title}
            draggable={draggable}
            onDragStart={this.drag}
            className={this.handleClass}
            onDoubleClick={this.handleDoubleClick}
          >
            <div className="threesizer--title">{name}</div>
          </div>
          <div
            className={`threesizer--contents ${orientation}`}
            style={this.contentStyle}
          >
            {render()}
          </div>
        </div>
      </>
    )
  }

  private get title() {
    return 'Drag to resize.\nDouble click to expand.'
  }

  private get contentStyle() {
    if (this.props.orientation === HANDLE_HORIZONTAL) {
      return {
        height: `calc(100% - ${this.handlePixels}px)`,
      }
    }

    return {
      width: `calc(100% - ${this.handlePixels}px)`,
    }
  }

  private get handleStyle() {
    const {handleDisplay: display} = this.props

    return {
      display,
    }
  }

  private get containerStyle() {
    if (this.props.orientation === HANDLE_HORIZONTAL) {
      return {
        height: this.size,
      }
    }

    return {
      width: this.size,
    }
  }

  private get size(): string {
    const {size, offset} = this.props
    return `calc((100% - ${offset}px) * ${size} + ${this.handlePixels}px)`
  }

  private get handlePixels(): number {
    if (this.props.handleDisplay === 'none') {
      return 0
    }

    return HANDLE_PIXELS
  }

  private get containerClass(): string {
    const {orientation} = this.props
    const isAnyHandleBeingDragged = !!this.props.activeHandleID
    return classnames('threesizer--division', {
      dragging: isAnyHandleBeingDragged,
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }

  private get handleClass(): string {
    const {draggable, orientation} = this.props

    return classnames('threesizer--handle', {
      disabled: !draggable,
      dragging: this.isDragging,
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
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
