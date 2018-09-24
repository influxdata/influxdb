// Libraries
import React, {Component} from 'react'
import classnames from 'classnames'

// Components
import FormLabel from 'src/clockface/components/form_layout/FormLabel'
import FormElementError from 'src/clockface/components/form_layout/FormElementError'
import FormHelpText from 'src/clockface/components/form_layout/FormHelpText'

// Types
import {Columns} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element
  label?: string
  helpText?: string
  errorMessage?: string
  colsXS?: Columns
  colsSM?: Columns
  colsMD?: Columns
  colsLG?: Columns
  offsetXS?: Columns
  offsetSM?: Columns
  offsetMD?: Columns
  offsetLG?: Columns
}

@ErrorHandling
class FormElement extends Component<Props> {
  public static defaultProps: Partial<Props> = {
    label: '',
    helpText: '',
    errorMessage: '',
    colsXS: Columns.Twelve,
  }

  public render() {
    const {children} = this.props
    return (
      <div className={this.className}>
        {this.groupLabel}
        {children}
        {this.errorMessage}
        {this.helpText}
      </div>
    )
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

    return classnames('form--element', {
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

  private get groupLabel(): JSX.Element {
    const {label} = this.props

    if (label) {
      return <FormLabel label={label} />
    }
  }

  private get helpText(): JSX.Element {
    const {helpText} = this.props

    if (helpText) {
      return <FormHelpText text={helpText} />
    }
  }

  private get errorMessage(): JSX.Element {
    const {errorMessage} = this.props

    if (errorMessage) {
      return <FormElementError message={errorMessage} />
    }
  }
}

export default FormElement
