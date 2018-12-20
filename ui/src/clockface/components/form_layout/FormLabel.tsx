// Libraries
import React, {Component} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  label: string
  children?: JSX.Element
}

@ErrorHandling
class FormLabel extends Component<Props> {
  public render() {
    const {label, children} = this.props

    return (
      <label className="form--label">
        {label}
        {children}
      </label>
    )
  }
}

export default FormLabel
