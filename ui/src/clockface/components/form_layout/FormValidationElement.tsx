// Libraries
import React, {Component} from 'react'

// Components
import FormLabel from 'src/clockface/components/form_layout/FormLabel'
import FormElementError from 'src/clockface/components/form_layout/FormElementError'
import FormHelpText from 'src/clockface/components/form_layout/FormHelpText'

// Types
import {ComponentStatus} from 'src/clockface/types'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: (status: ComponentStatus) => JSX.Element
  validationFunc: (value: any) => string | null
  onStatusChange?: (newStatus: ComponentStatus) => void
  labelAddOn?: () => JSX.Element
  label: string
  value: any
  helpText?: string
  required?: boolean
}

interface State {
  errorMessage: string | null
  status: ComponentStatus
}

@ErrorHandling
class FormValidationElement extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      errorMessage: null,
      status: ComponentStatus.Default,
    }
  }

  public componentDidUpdate(prevProps: Props, prevState: State) {
    if (prevProps.value === this.props.value) {
      return
    }

    const {validationFunc, onStatusChange} = this.props
    const errorMessage = validationFunc(this.props.value)
    const newStatus = errorMessage
      ? ComponentStatus.Error
      : ComponentStatus.Valid

    if (onStatusChange && prevState.status !== newStatus) {
      onStatusChange(newStatus)
    }

    this.setState({status: newStatus, errorMessage})
  }

  public render() {
    return (
      <div className="form--element">
        {this.label}
        {this.children}
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

  private get children(): JSX.Element {
    const {children} = this.props
    const {status} = this.state

    return children(status)
  }

  private get errorMessage(): JSX.Element {
    const {errorMessage} = this.state

    if (errorMessage) {
      return <FormElementError message={errorMessage} />
    }
  }
}

export default FormValidationElement
