// Libraries
import React, {Component, ComponentClass} from 'react'
import _ from 'lodash'
import classnames from 'classnames'

// Components
import FormElement from 'src/clockface/components/form_layout/FormElement'
import FormLabel from 'src/clockface/components/form_layout/FormLabel'
import FormDivider from 'src/clockface/components/form_layout/FormDivider'
import FormFooter from 'src/clockface/components/form_layout/FormFooter'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
  style?: React.CSSProperties
  className?: string
  onSubmit?: (e: React.FormEvent) => void
}

@ErrorHandling
class Form extends Component<Props> {
  public static Element = FormElement
  public static Label = FormLabel
  public static Divider = FormDivider
  public static Footer = FormFooter

  public static ValidChildTypes: ComponentClass[] = [
    FormElement,
    FormLabel,
    FormDivider,
    FormFooter,
  ]

  public render() {
    const {children, style} = this.props

    return (
      <form
        style={style}
        className={this.formWrapperClass}
        onSubmit={this.handleSubmit}
      >
        {children}
      </form>
    )
  }

  private handleSubmit = (e: React.FormEvent): void => {
    e.preventDefault()
    const {onSubmit} = this.props

    if (onSubmit) {
      onSubmit(e)
    }
  }

  private get formWrapperClass(): string {
    const {className} = this.props

    return classnames('form--wrapper', {
      [`${className}`]: className,
    })
  }
}

export default Form
