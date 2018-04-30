import React, {PureComponent, ReactElement} from 'react'
import classnames from 'classnames'
import ResizeHandle from 'src/shared/components/ResizeHandle'

import {
  ORIENTATION_VERTICAL,
  ORIENTATION_HORIZONTAL,
  HUNDRED,
} from 'src/shared/constants/'

interface Props {
  id: string
  name?: string
  size: number
  offset: number
  isDragging: boolean
  draggable: boolean
  orientation: string
  render: () => ReactElement<any>
  onHandleStartDrag: () => void
}

class Division extends PureComponent<Props> {
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
    const {
      name,
      isDragging,
      orientation,
      onHandleStartDrag,
      draggable,
    } = this.props

    if (name) {
      return <div className="resizer--title">{name}</div>
    }

    if (draggable) {
      return (
        <ResizeHandle
          onHandleStartDrag={onHandleStartDrag}
          orientation={orientation}
          isDragging={isDragging}
        />
      )
    }
  }

  private get style() {
    const {orientation, size, offset} = this.props

    const sizePercent = `${size * HUNDRED}%`
    const offsetPercent = `${offset * HUNDRED}%`

    if (orientation === ORIENTATION_VERTICAL) {
      return {
        top: '0',
        width: sizePercent,
        left: offsetPercent,
      }
    }

    return {
      left: '0',
      height: sizePercent,
      top: offsetPercent,
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
