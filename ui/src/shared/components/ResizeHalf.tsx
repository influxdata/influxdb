import React, {PureComponent, ReactElement} from 'react'
import classnames from 'classnames'

import {
  HANDLE_VERTICAL,
  HANDLE_HORIZONTAL,
  HUNDRED,
} from 'src/shared/constants/'

interface Props {
  percent: number
  minPixels: number
  component: ReactElement<any>
  orientation: string
  offset: number
}

class ResizeHalf extends PureComponent<Props> {
  public render() {
    const {component} = this.props

    return (
      <div className={this.className} style={this.style}>
        {component}
      </div>
    )
  }

  private get style() {
    const {orientation, minPixels, percent, offset} = this.props

    const size = `${percent * HUNDRED}%`
    const gap = `${offset * HUNDRED}%`

    if (orientation === HANDLE_VERTICAL) {
      return {
        top: '0',
        width: size,
        left: gap,
        minWidth: minPixels,
      }
    }

    return {
      left: '0',
      height: size,
      top: gap,
      minHeight: minPixels,
    }
  }

  private get className(): string {
    const {orientation} = this.props

    return classnames('resize--half', {
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }
}

export default ResizeHalf
