import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Copy
import {invalidJSON} from 'src/shared/copy/notifications'

// Actions
import {
  createVariableFromTemplate as createVariableFromTemplateAction,
  getVariables as getVariablesAction,
} from 'src/variables/actions/thunks'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {ComponentStatus} from '@influxdata/clockface'

// Utils
import jsonlint from 'jsonlint-mod'

interface State {
  status: ComponentStatus
}

interface DispatchProps {
  createVariableFromTemplate: typeof createVariableFromTemplateAction
  getVariables: typeof getVariablesAction
  notify: typeof notifyAction
}

type Props = DispatchProps & WithRouterProps

class VariableImportOverlay extends PureComponent<Props> {
  public state: State = {
    status: ComponentStatus.Default,
  }

  public render() {
    return (
      <ImportOverlay
        onDismissOverlay={this.onDismiss}
        resourceName="Variable"
        onSubmit={this.handleImportVariable}
        status={this.state.status}
        updateStatus={this.updateOverlayStatus}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }

  private updateOverlayStatus = (status: ComponentStatus) =>
    this.setState(() => ({status}))

  private handleImportVariable = (uploadContent: string) => {
    const {createVariableFromTemplate, getVariables, notify} = this.props

    let template
    this.updateOverlayStatus(ComponentStatus.Default)
    try {
      template = jsonlint.parse(uploadContent)
    } catch (error) {
      this.updateOverlayStatus(ComponentStatus.Error)
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
