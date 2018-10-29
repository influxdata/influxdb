// Libraries
import React, {Component} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children: JSX.Element[] | JSX.Element
}

@ErrorHandling
class PanelBody extends Component<Props> {
  public render() {
    const {children} = this.props

    return <div className="panel-body">{children}</div>
  }
}

export default PanelBody
