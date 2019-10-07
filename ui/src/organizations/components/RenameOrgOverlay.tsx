// Libraries
import React, {PureComponent} from 'react'
import _ from 'lodash'

// Components
import DangerConfirmationOverlay from 'src/shared/components/dangerConfirmation/DangerConfirmationOverlay'
import RenameOrgForm from 'src/organizations/components/RenameOrgForm'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  onDismiss: () => void
}

@ErrorHandling
class RenameOrgOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss} = this.props
  
    return (
      <DangerConfirmationOverlay
        title="Rename Organization"
        message={this.message}
        effectedItems={this.effectedItems}
        onClose={onDismiss}
        confirmButtonText="I understand, let's rename my Organization"
      >
        <RenameOrgForm onDismiss={onDismiss} />
      </DangerConfirmationOverlay>
    )
  }

  private get message(): string {
    return 'Updating the name of an Organization can have unintended consequences. Anything that references this Organization by name will stop working including:'
  }

  private get effectedItems(): string[] {
    return [
      'Queries',
      'Dashboards',
      'Tasks',
      'Telegraf Configurations',
      'Client Libraries',
    ]
  }
}

export default RenameOrgOverlay
