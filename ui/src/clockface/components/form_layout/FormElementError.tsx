// Libraries
import React, {Component} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  message: string
}

@ErrorHandling
class FormElementError extends Component<Props> {
  public render() {
    const {message} = this.props

    return <span className="form--element-error">{message}</span>
  }
}

export default FormElementError
