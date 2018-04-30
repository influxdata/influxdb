import React, {PureComponent, ReactElement} from 'react'
import classnames from 'classnames'

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
  orientation: string
  render: () => ReactElement<any>
}

class Division extends PureComponent<Props> {
  public render() {
    const {name, render} = this.props

    return (
      <div className={this.className} style={this.style}>
        {name && <div>{name}</div>}
        {render()}
      </div>
    )
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
