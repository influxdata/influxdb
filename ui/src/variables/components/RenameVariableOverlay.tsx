// Libraries
import React, {PureComponent} from 'react'

import _ from 'lodash'

// Components
import DangerConfirmationOverlay from 'src/shared/components/dangerConfirmation/DangerConfirmationOverlay'
import RenameVariableForm from 'src/variables/components/RenameVariableForm'

// Decorators
import {ErrorHandling} from 'src/shared/decorators/errors'

interface Props {
  variableID: string
  onDismiss: () => void
}

@ErrorHandling
class RenameVariableOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss, variableID} = this.props

    return (
      <DangerConfirmationOverlay
        title="Rename Variable"
        message={this.message}
        effectedItems={this.effectedItems}
        onClose={onDismiss}
        confirmButtonText="I understand, let's rename my Variable"
      >
        <RenameVariableForm onDismiss={onDismiss} variableID={variableID} />
      </DangerConfirmationOverlay>
    )
  }

  private get message(): string {
    return 'Updating the name of a Variable can have unintended consequences. Anything that references this Variable by name will stop working including:'
  }

  private get effectedItems(): string[] {
    return ['Queries', 'Dashboards', 'Telegraf Configurations', 'Templates']
  }
}

export default RenameVariableOverlay
