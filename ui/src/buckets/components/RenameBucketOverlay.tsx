// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import DangerConfirmationOverlay from 'src/shared/components/dangerConfirmation/DangerConfirmationOverlay'
import RenameBucketForm from 'src/buckets/components/RenameBucketForm'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onDismiss: () => void
  bucketID: string
}

@ErrorHandling
class RenameBucketOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss, bucketID} = this.props

    return (
      <DangerConfirmationOverlay
        title="Rename Bucket"
        message={this.message}
        effectedItems={this.effectedItems}
        onClose={onDismiss}
        confirmButtonText="I understand, let's rename my Bucket"
      >
        <RenameBucketForm bucketID={bucketID} onDismiss={onDismiss} />
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
}

export default RenameBucketOverlay
