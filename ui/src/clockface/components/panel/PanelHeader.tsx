// Libraries
import React, {Component} from 'react'

// Components
import {
  ComponentSpacer,
  FlexDirection,
  JustifyContent,
  ComponentSize,
} from '@influxdata/clockface'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  children?: JSX.Element[] | JSX.Element
  title: string
}

@ErrorHandling
class PanelHeader extends Component<Props> {
  public render() {
    const {children, title} = this.props

    return (
      <div className="panel-header">
        <div className="panel-title">{title}</div>
        <div className="panel-controls">
          <ComponentSpacer
            margin={ComponentSize.Small}
            direction={FlexDirection.Row}
            justifyContent={JustifyContent.FlexEnd}
          >
            {children}
          </ComponentSpacer>
        </div>
      </div>
    )
  }
}

export default PanelHeader
