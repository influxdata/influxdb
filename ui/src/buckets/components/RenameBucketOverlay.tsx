// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'

import _ from 'lodash'

// Components
import DangerConfirmationOverlay from 'src/shared/components/dangerConfirmation/DangerConfirmationOverlay'
import RenameBucketForm from 'src/buckets/components/RenameBucketForm'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class RenameBucketOverlay extends PureComponent<WithRouterProps> {
  public render() {
    return (
      <DangerConfirmationOverlay
        title="Rename Bucket"
        message={this.message}
        effectedItems={this.effectedItems}
        onClose={this.handleClose}
        confirmButtonText="I understand, let's rename my Bucket"
      >
        <RenameBucketForm />
      </DangerConfirmationOverlay>
    )
  }

  private get message(): string {
    return 'Updating the name of a Bucket can have unintended consequences. Anything that references this Bucket by name will stop working including:'
  }

  private get effectedItems(): string[] {
    return [
      'Queries',
      'Dashboards',
      'Tasks',
      'Telegraf Configurations',
      'Templates',
    ]
  }

  private handleClose = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/load-data/buckets`)
  }
}

export default withRouter(RenameBucketOverlay)
