import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Copy
import {invalidJSON} from 'src/shared/copy/notifications'

// Actions
import {
  createVariableFromTemplate as createVariableFromTemplateAction,
  getVariables as getVariablesAction,
} from 'src/variables/actions'

import {notify as notifyAction} from 'src/shared/actions/notifications'

interface DispatchProps {
  createVariableFromTemplate: typeof createVariableFromTemplateAction
  getVariables: typeof getVariablesAction
  notify: typeof notifyAction
}

type Props = DispatchProps & WithRouterProps

class VariableImportOverlay extends PureComponent<Props> {
  public render() {
    return (
      <ImportOverlay
        onDismissOverlay={this.onDismiss}
        resourceName="Variable"
        onSubmit={this.handleImportVariable}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }

  private handleImportVariable = (uploadContent: string) => {
    const {createVariableFromTemplate, getVariables, notify} = this.props

    let template
    try {
      template = JSON.parse(uploadContent)
    } catch (error) {
      notify(invalidJSON(error.message))
      return
    }

    createVariableFromTemplate(template)
    getVariables()

    this.onDismiss()
  }
}

const mdtp: DispatchProps = {
  createVariableFromTemplate: createVariableFromTemplateAction,
  getVariables: getVariablesAction,
  notify: notifyAction,
}

export default connect<{}, DispatchProps, Props>(
  null,
  mdtp
)(withRouter(VariableImportOverlay))
