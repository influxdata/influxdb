// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Types
import {Columns} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface OwnProps {
  children: JSX.Element | JSX.Element[]
}

interface DefaultProps {
  colsXS?: Columns
  colsSM?: Columns
  colsMD?: Columns
  colsLG?: Columns
  offsetXS?: Columns
  offsetSM?: Columns
  offsetMD?: Columns
  offsetLG?: Columns
}

type Props = DefaultProps & OwnProps

@ErrorHandling
class FormFooter extends Component<Props> {
  public static defaultProps: DefaultProps = {
    colsXS: Columns.Twelve,
  }

  public render() {
    const {children} = this.props
    return <div className={this.className}>{children}</div>
  }

  private get className(): string {
    const {
      colsXS,
      colsSM,
      colsMD,
      colsLG,
      offsetXS,
      offsetSM,
      offsetMD,
      offsetLG,
    } = this.props

    return classnames('form--element form--submit', {
      [`col-xs-${colsXS}`]: colsXS,
      [`col-sm-${colsSM}`]: colsSM,
      [`col-md-${colsMD}`]: colsMD,
      [`col-lg-${colsLG}`]: colsLG,
      [`col-xs-offset-${offsetXS}`]: offsetXS,
      [`col-sm-offset-${offsetSM}`]: offsetSM,
      [`col-md-offset-${offsetMD}`]: offsetMD,
      [`col-lg-offset-${offsetLG}`]: offsetLG,
    })
  }
}

export default FormFooter
