import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {
  createVariableFromTemplate as createVariableFromTemplateAction,
  getVariables as getVariablesAction,
} from 'src/variables/actions'

interface DispatchProps {
  createVariableFromTemplate: typeof createVariableFromTemplateAction
  getVariables: typeof getVariablesAction
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

  private handleImportVariable = async (
    uploadContent: string
  ): Promise<void> => {
    const {createVariableFromTemplate, getVariables} = this.props

    const template = JSON.parse(uploadContent)
    await createVariableFromTemplate(template)
    getVariables()

    this.onDismiss()
  }
}

const mdtp: DispatchProps = {
  createVariableFromTemplate: createVariableFromTemplateAction,
  getVariables: getVariablesAction,
}

export default connect<{}, DispatchProps, Props>(
  null,
  mdtp
)(withRouter(VariableImportOverlay))
