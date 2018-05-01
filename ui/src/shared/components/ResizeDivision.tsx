import React, {PureComponent, ReactElement} from 'react'
import classnames from 'classnames'
import ResizeHandle, {
  OnHandleStartDrag,
} from 'src/shared/components/ResizeHandle'

import {
  HANDLE_VERTICAL,
  HANDLE_HORIZONTAL,
  HUNDRED,
} from 'src/shared/constants/'

const NOOP = () => {}

interface Props {
  id: string
  name?: string
  minPixels: number
  size: number
  activeHandleID: string
  draggable: boolean
  orientation: string
  render: () => ReactElement<any>
  onHandleStartDrag: OnHandleStartDrag
  minPercent: (pixels: number) => number
  maxPercent: number
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
        <div className="resizer--contents">{render()}</div>
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

  private get minPercent(): number {
    const {minPercent, minPixels} = this.props

    return minPercent(minPixels)
  }

  private get style() {
    const {orientation, size, maxPercent} = this.props

    const sizePercent = `${size * HUNDRED}%`
    const min = `${this.minPercent * HUNDRED}%`
    const max = `${maxPercent * HUNDRED}%`

    if (orientation === HANDLE_VERTICAL) {
      return {
        top: '0',
        width: sizePercent,
        minWidth: min,
        maxWidth: max,
      }
    }

    return {
      left: '0',
      height: sizePercent,
      minHeight: min,
      maxHeight: max,
    }
  }

  private get className(): string {
    const {orientation} = this.props
    // todo use constants instead of "vertical" / "horizontal"
    return classnames('resizer--division', {
      resizer__vertical: orientation === HANDLE_VERTICAL,
      resizer__horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }
}

export default Division
