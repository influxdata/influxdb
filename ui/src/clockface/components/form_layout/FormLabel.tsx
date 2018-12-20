// Libraries
import React, {Component} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  label: string
  children?: JSX.Element
  required?: boolean
}

@ErrorHandling
class FormLabel extends Component<Props> {
  public render() {
    const {label, children} = this.props

    return (
      <label className="form--label">
        <span>
          {label}
          {this.requiredIndicator}
        </span>
        {children}
      </label>
    )
  }

  private get requiredIndicator(): JSX.Element {
    const {required} = this.props

    if (required) {
      return <span className="form--label-required">*</span>
    }
  }
}

export default FormLabel
