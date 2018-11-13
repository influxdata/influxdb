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

  public static ValidChildNames: string = _.map(Form.ValidChildTypes, valid => {
    const name = valid.displayName.split('Form').pop()

    return `<Form.${name}>`
  }).join(', ')

  public render() {
    const {children, style} = this.props

    this.validateChildren()

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
    this.props.onSubmit(e)
  }

  private get formWrapperClass(): string {
    const {className} = this.props

    return classnames('form--wrapper', {
      [`${className}`]: className,
    })
  }

  private validateChildren = (): void => {
    const childArray = React.Children.toArray(this.props.children)
    const childrenAreValid = _.every(childArray, this.childTypeIsValid)
    if (!childrenAreValid) {
      // throw new Error(
      //   `<Form> expected children of type ${Form.ValidChildNames}`
      // )
    }
  }

  private childTypeIsValid = (child: JSX.Element): boolean =>
    _.includes(Form.ValidChildTypes, child.type)
}

export default Form
