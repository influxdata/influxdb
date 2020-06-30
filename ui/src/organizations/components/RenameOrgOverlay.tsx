// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'

import _ from 'lodash'

// Components
import DangerConfirmationOverlay from 'src/shared/components/dangerConfirmation/DangerConfirmationOverlay'
import RenameOrgForm from 'src/organizations/components/RenameOrgForm'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class RenameOrgOverlay extends PureComponent<WithRouterProps> {
  public render() {
    return (
      <DangerConfirmationOverlay
        title="Rename Organization"
        message={this.message}
        effectedItems={this.effectedItems}
        onClose={this.handleClose}
        confirmButtonText="I understand, let's rename my Organization"
      >
        <RenameOrgForm />
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

  private handleClose = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/orgs/${orgID}/settings/about`)
  }
}

export default withRouter(RenameOrgOverlay)
