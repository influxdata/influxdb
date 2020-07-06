// Libraries
import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'

import _ from 'lodash'

// Components
import DangerConfirmationOverlay from 'src/shared/components/dangerConfirmation/DangerConfirmationOverlay'
import RenameVariableForm from 'src/variables/components/RenameVariableForm'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

@ErrorHandling
class RenameVariableOverlay extends PureComponent<
  RouteComponentProps<{orgID: string}>
> {
  public render() {
    return (
      <DangerConfirmationOverlay
        title="Rename Variable"
        message={this.message}
        effectedItems={this.effectedItems}
        onClose={this.handleClose}
        confirmButtonText="I understand, let's rename my Variable"
      >
        <RenameVariableForm onClose={this.handleClose} />
      </DangerConfirmationOverlay>
    )
  }

  private get message(): string {
    return 'Updating the name of a Variable can have unintended consequences. Anything that references this Variable by name will stop working including:'
  }

  private get effectedItems(): string[] {
    return ['Queries', 'Dashboards', 'Telegraf Configurations', 'Templates']
  }

  private handleClose = () => {
    const {history, match} = this.props

    history.push(`/orgs/${match.params.orgID}/settings/variables`)
  }
}

export default withRouter(RenameVariableOverlay)
