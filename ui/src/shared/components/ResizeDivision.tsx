import React, {PureComponent, ReactElement} from 'react'
import classnames from 'classnames'
import ResizeHandle from 'src/shared/components/ResizeHandle'

import {
  ORIENTATION_VERTICAL,
  ORIENTATION_HORIZONTAL,
  HUNDRED,
} from 'src/shared/constants/'

const NOOP = () => {}

interface Props {
  id: string
  name?: string
  size: number
  activeHandleID: string
  draggable: boolean
  orientation: string
  render: () => ReactElement<any>
  onHandleStartDrag: (activeHandleID: string) => void
}

class Division extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    name: '',
  }

  public render() {
    const {render} = this.props

    return (
      <div className={this.className} style={this.style}>
        {this.dragHandle}
        {render()}
      </div>
    )
  }

  private get dragHandle() {
    const {name, activeHandleID, orientation, id, draggable} = this.props

    if (!name && !draggable) {
      return null
    }

    return (
      <ResizeHandle
        id={id}
        name={name}
        orientation={orientation}
        activeHandleID={activeHandleID}
        onHandleStartDrag={this.dragCallback}
      />
    )
  }

  private get dragCallback() {
    const {draggable} = this.props
    if (!draggable) {
      return NOOP
    }

    return this.props.onHandleStartDrag
  }

  private get style() {
    const {orientation, size} = this.props

    const sizePercent = `${size * HUNDRED}%`

    if (orientation === ORIENTATION_VERTICAL) {
      return {
        top: '0',
        width: sizePercent,
      }
    }

    return {
      left: '0',
      height: sizePercent,
    }
  }

  private get className(): string {
    const {orientation} = this.props
    // todo use constants instead of "vertical" / "horizontal"
    return classnames('resizer--division', {
      resizer__vertical: orientation === ORIENTATION_VERTICAL,
      resizer__horizontal: orientation === ORIENTATION_HORIZONTAL,
    })
  }
}

export default Division
