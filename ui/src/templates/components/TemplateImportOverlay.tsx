import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {
  createTemplate as createTemplateAction,
  setTemplatesStatus as setTemplatesStatusAction,
} from 'src/templates/actions'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState, Organization, RemoteDataState} from 'src/types'

interface DispatchProps {
  createTemplate: typeof createTemplateAction
  setTemplatesStatus: typeof setTemplatesStatusAction
  notify: typeof notifyAction
}

interface StateProps {
  org: Organization
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string}
}

type Props = DispatchProps & OwnProps & StateProps

class TemplateImportOverlay extends PureComponent<Props> {
  public render() {
    return (
      <ImportOverlay
        onDismissOverlay={this.onDismiss}
        resourceName="Template"
        onSubmit={this.handleImportTemplate}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }

  private handleImportTemplate = () => async (
    importString: string
  ): Promise<void> => {
    const {createTemplate} = this.props
    const {setTemplatesStatus} = this.props

    const template = JSON.parse(importString)
    await createTemplate(template)
    setTemplatesStatus(RemoteDataState.NotStarted)

    this.onDismiss()
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const {
    orgs: {items},
  } = state

  const org = items.find(o => o.id === props.params.orgID)

  return {org}
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createTemplate: createTemplateAction,
  setTemplatesStatus: setTemplatesStatusAction,
}

export default connect<StateProps, DispatchProps, Props>(
  mstp,
  mdtp
)(withRouter(TemplateImportOverlay))
