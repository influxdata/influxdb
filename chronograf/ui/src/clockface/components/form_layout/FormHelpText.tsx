// Libraries
import React, {Component} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  text: string
}

@ErrorHandling
class FormHelpText extends Component<Props> {
  public render() {
    const {text} = this.props

    return <span className="form--help-text">{text}</span>
  }
}

export default FormHelpText
