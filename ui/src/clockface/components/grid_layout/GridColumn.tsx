// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {Columns} from 'src/clockface/types'

interface Props {
  children: JSX.Element[] | JSX.Element
  widthXS?: Columns
  widthSM?: Columns
  widthMD?: Columns
  widthLG?: Columns
  offsetXS?: Columns
  offsetSM?: Columns
  offsetMD?: Columns
  offsetLG?: Columns
}

class GridColumn extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    widthXS: Columns.Twelve,
  }

  public render() {
    const {children} = this.props

    return <div className={this.className}>{children}</div>
  }

  private get className(): string {
    const {
      widthXS,
      widthSM,
      widthMD,
      widthLG,
      offsetXS,
      offsetSM,
      offsetMD,
      offsetLG,
    } = this.props

    return classnames('grid--column', {
      [`col-xs-${widthXS}`]: widthXS,
      [`col-sm-${widthSM}`]: widthSM,
      [`col-md-${widthMD}`]: widthMD,
      [`col-lg-${widthLG}`]: widthLG,
      [`col-xs-offset-${offsetXS}`]: offsetXS,
      [`col-sm-offset-${offsetSM}`]: offsetSM,
      [`col-md-offset-${offsetMD}`]: offsetMD,
      [`col-lg-offset-${offsetLG}`]: offsetLG,
    })
  }
}

export default GridColumn
