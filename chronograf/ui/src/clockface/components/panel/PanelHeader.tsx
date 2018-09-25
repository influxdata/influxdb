// Libraries
import React, {Component} from 'react'

import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children?: JSX.Element[]
  title: string
}

@ErrorHandling
class PanelHeader extends Component<Props> {
  public render() {
    const {children, title} = this.props

    return (
      <div className="panel-header">
        <div className="panel-title">{title}</div>
        <div className="panel-controls">{children}</div>
      </div>
    )
  }
}

export default PanelHeader
