import React, {PureComponent, MouseEvent} from 'react'
import classnames from 'classnames'

import {HANDLE_VERTICAL, HANDLE_HORIZONTAL} from 'src/shared/constants/'

export type OnHandleStartDrag = (
  activeHandleID: string,
  e: MouseEvent<HTMLElement>
) => void

interface Props {
  name: string
  id: string
  onHandleStartDrag: OnHandleStartDrag
  activeHandleID: string
  orientation: string
}

class ResizeHandle extends PureComponent<Props> {
  public render() {
    return (
      <div className={this.className} onMouseDown={this.handleMouseDown}>
        {this.props.name}
      </div>
    )
  }

  private get className(): string {
    const {name, orientation} = this.props

    return classnames({
      'resizer--handle': !name,
      'resizer--title': name,
      dragging: this.isActive,
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }

  private get isActive(): boolean {
    const {id, activeHandleID} = this.props

    return id === activeHandleID
  }

  private handleMouseDown = (e: MouseEvent<HTMLElement>): void => {
    this.props.onHandleStartDrag(this.props.id, e)
  }
}

export default ResizeHandle
