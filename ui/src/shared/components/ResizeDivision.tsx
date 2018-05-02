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
  maxPercent: number
}

class Division extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    name: '',
  }

  public render() {
    const {render, draggable} = this.props

    if (!name && !draggable) {
      return null
    }

    return (
      <>
        <div className="threesizer--handle" onMouseDown={this.dragCallback}>
          {name}
        </div>
        <div className="threesizer--division" style={this.style}>
          {render()}
        </div>
      </>
    )
  }

  private get style() {
    return {
      height: `calc(${this.props.size}% - 30px)`,
    }
  }

  private get dragCallback() {
    const {draggable} = this.props
    if (!draggable) {
      return NOOP
    }

    return this.props.onHandleStartDrag
  }
}

export default Division
