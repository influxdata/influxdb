// Libraries
import React, {Component} from 'react'

// Components
import FormLabel from 'src/clockface/components/form_layout/FormLabel'
import FormElementError from 'src/clockface/components/form_layout/FormElementError'
import FormHelpText from 'src/clockface/components/form_layout/FormHelpText'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element
  label: string
  helpText?: string
  errorMessage?: string
  labelAddOn?: () => JSX.Element
  required?: boolean
}

@ErrorHandling
class FormElement extends Component<Props> {
  public render() {
    const {children} = this.props
    return (
      <div className="form--element">
        {this.label}
        {children}
        {this.errorMessage}
        {this.helpText}
      </div>
    )
  }

  private get label(): JSX.Element {
    const {label, required} = this.props

    if (label) {
      return (
        <FormLabel label={label} required={required}>
          {this.labelChild}
        </FormLabel>
      )
    }
  }

  private get labelChild(): JSX.Element {
    const {labelAddOn} = this.props

    if (labelAddOn) {
      return labelAddOn()
    }
  }

  private get helpText(): JSX.Element {
    const {helpText} = this.props

    if (helpText) {
      return <FormHelpText text={helpText} />
    }
  }

  private get errorMessage(): JSX.Element {
    if ('errorMessage' in this.props) {
      const {errorMessage} = this.props
      return <FormElementError message={errorMessage} />
    }
  }
}

export default FormElement
