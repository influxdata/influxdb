// Libraries
import React, {SFC, Component} from 'react'
import classnames from 'classnames'

// Components
import FormElement from 'src/clockface/components/form_layout/FormElement'
import FormValidationElement from 'src/clockface/components/form_layout/FormValidationElement'
import FormLabel from 'src/clockface/components/form_layout/FormLabel'
import FormDivider from 'src/clockface/components/form_layout/FormDivider'
import FormFooter from 'src/clockface/components/form_layout/FormFooter'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
  style?: React.CSSProperties
  className?: string
  onSubmit?: (e: React.FormEvent) => void
  testID: string
}

interface BoxProps {
  children: JSX.Element | JSX.Element[]
  className?: string
}

@ErrorHandling
class Form extends Component<Props> {
  public static ValidationElement = FormValidationElement
  public static Element = FormElement
  public static Label = FormLabel
  public static Divider = FormDivider
  public static Footer = FormFooter

  public static defaultProps = {
    testID: 'form-container',
  }

  public static Box: SFC<BoxProps> = ({children, className = ''}) => (
    <div className={`form--box ${className}`}>{children}</div>
  )

  public render() {
    const {children, style, testID} = this.props

    return (
      <form
        style={style}
        className={this.formWrapperClass}
        onSubmit={this.handleSubmit}
        data-testid={testID}
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
