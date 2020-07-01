// Libraries
import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import _ from 'lodash'

// Components
import {Button, ComponentColor, ComponentSize} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Dashboard} from 'src/types'

interface OwnProps {
  dashboards: Dashboard[]
  onExit: () => void
}

type Props = OwnProps & RouteComponentProps

@ErrorHandling
class CompletionQuickStartButton extends PureComponent<Props> {
  public render() {
    return (
      <Button
        text="Quick Start"
        color={ComponentColor.Success}
        size={ComponentSize.Large}
        onClick={this.handleAdvanced}
        testID="button--quick-start"
      />
    )
  }

  private handleAdvanced = (): void => {
    const {history, dashboards, onExit} = this.props
    const id = _.get(dashboards, '[0].id', null)
    if (id) {
      history.push(`/dashboards/${id}`)
    } else {
      onExit()
    }
  }
}

export default withRouter(CompletionQuickStartButton)
