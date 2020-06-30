// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
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

type Props = OwnProps & WithRouterProps

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
    const {router, dashboards, onExit} = this.props
    const id = _.get(dashboards, '[0].id', null)
    if (id) {
      router.push(`/dashboards/${id}`)
    } else {
      onExit()
    }
  }
}

export default withRouter<OwnProps>(CompletionQuickStartButton)
