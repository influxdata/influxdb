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
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState, Organization} from 'src/types'

interface DispatchProps {
  getVariables: typeof getVariablesAction
  createVariableFromTemplate: typeof createVariableFromTemplateAction
  notify: typeof notifyAction
}

interface StateProps {
  org: Organization
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string}
}

type Props = DispatchProps & OwnProps & StateProps

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
    uploadContent: string,
    orgID: string
  ): Promise<void> => {
    const {createVariableFromTemplate, getVariables} = this.props

    const template = JSON.parse(uploadContent)
    await createVariableFromTemplate(template, orgID)

    await getVariables()

    this.onDismiss()
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const {orgs} = state

  const org = orgs.find(o => o.id === props.params.orgID)

  return {org}
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createVariableFromTemplate: createVariableFromTemplateAction,
  getVariables: getVariablesAction,
}

export default connect<StateProps, DispatchProps, Props>(
  mstp,
  mdtp
)(withRouter(VariableImportOverlay))
