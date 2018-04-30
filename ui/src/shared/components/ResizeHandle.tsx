import React, {PureComponent} from 'react'
import classnames from 'classnames'

import {
  ORIENTATION_VERTICAL,
  ORIENTATION_HORIZONTAL,
} from 'src/shared/constants/'

interface Props {
  name: string
  id: string
  onHandleStartDrag: (activeHandleID: string) => void
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
      vertical: orientation === ORIENTATION_VERTICAL,
      horizontal: orientation === ORIENTATION_HORIZONTAL,
    })
  }

  private get isActive(): boolean {
    const {id, activeHandleID} = this.props

    return id === activeHandleID
  }

  private handleMouseDown = (): void => {
    this.props.onHandleStartDrag(this.props.id)
  }
}

export default ResizeHandle
