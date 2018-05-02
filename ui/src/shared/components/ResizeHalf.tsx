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
  render: () => ReactElement<any>
  orientation: string
}

class ResizerHalf extends PureComponent<Props> {
  public render() {
    const {render} = this.props

    return (
      <div className={this.className} style={this.style}>
        {render()}
      </div>
    )
  }

  private get style() {
    const {orientation, minPixels, percent} = this.props

    const size = `${percent * HUNDRED}%`

    if (orientation === HANDLE_VERTICAL) {
      return {
        top: '0',
        width: size,
        minWidth: minPixels,
      }
    }

    return {
      left: '0',
      height: size,
      minHeight: minPixels,
    }
  }

  private get className(): string {
    const {orientation} = this.props

    return classnames('resizer--half', {
      vertical: orientation === HANDLE_VERTICAL,
      horizontal: orientation === HANDLE_HORIZONTAL,
    })
  }
}

export default ResizerHalf
