// Libraries
import React, {Component} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  message: string
}

@ErrorHandling
class FormElementError extends Component<Props> {
  public render() {
    return <span className="form--element-error">{this.message}</span>
  }

  private get message() {
    const {message} = this.props

    // TODO(watts): temporary workaround for: https://github.com/influxdata/influxdb/issues/11372
    if (!message) {
      return '\u00a0\u00a0'
    }

    return this.props.message
  }
}

export default FormElementError
