// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import _ from 'lodash'

// Components
import {Button, ComponentColor, ComponentSize} from '@influxdata/clockface'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Organization} from 'src/types'

interface OwnProps {
  orgs: Organization[]
  onExit: () => void
}

type Props = OwnProps & WithRouterProps

@ErrorHandling
class CompletionAdvancedButton extends PureComponent<Props> {
  public render() {
    return (
      <Button
        text="Advanced"
        color={ComponentColor.Success}
        size={ComponentSize.Large}
        onClick={this.handleAdvanced}
        testID="button--advanced"
      />
    )
  }

  private handleAdvanced = (): void => {
    const {router, orgs, onExit} = this.props
    const id = _.get(orgs, '0.id', null)
    if (id) {
      router.push(`/orgs/${id}/load-data/buckets`)
    } else {
      onExit()
    }
  }
}

export default withRouter<OwnProps>(CompletionAdvancedButton)
