import React, {PureComponent, ReactElement, MouseEvent} from 'react'
import classnames from 'classnames'

import FancyScrollbar from 'src/shared/components/FancyScrollbar'
import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants/index'

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
    const {name, render, orientation} = this.props
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
            <div className="threesizer--title">{name}</div>
          </div>
          <FancyScrollbar
            className={`threesizer--contents ${orientation}`}
            autoHide={true}
          >
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
    return `calc((100% - ${offset}px) * ${size} + 30px)`
  }

  private get containerClass(): string {
    const isAnyHandleBeingDragged = !!this.props.activeHandleID
    return classnames('threesizer--division', {
      dragging: isAnyHandleBeingDragged,
    })
  }

  private get className(): string {
    const {draggable, orientation} = this.props

    return classnames('threesizer--handle', {
      disabled: !draggable,
      dragging: this.isDragging,
      vertical: orientation === HANDLE_VERTICAL,
      horizonatl: orientation === HANDLE_HORIZONTAL,
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
