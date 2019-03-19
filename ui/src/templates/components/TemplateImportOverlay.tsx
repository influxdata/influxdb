import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {
  createTemplate as createTemplateAction,
  getTemplatesForOrg as getTemplatesForOrgAction,
} from 'src/templates/actions'
import {AppState, Organization} from 'src/types/v2'

interface DispatchProps {
  createTemplate: typeof createTemplateAction
  getTemplatesForOrg: typeof getTemplatesForOrgAction
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
    const {org} = this.props
    return (
      <ImportOverlay
        onDismissOverlay={this.onDismiss}
        resourceName="Template"
        onSubmit={this.handleImportTemplate(org.name)}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }

  private handleImportTemplate = (orgName: string) => async (
    importString: string
  ): Promise<void> => {
    const {createTemplate, getTemplatesForOrg} = this.props

    try {
      const template = JSON.parse(importString)

      await createTemplate(template)

      getTemplatesForOrg(orgName)
    } catch (error) {}

    this.onDismiss()
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const {orgs} = state

  const org = orgs.find(o => o.id === props.params.orgID)

  return {org}
}

const mdtp: DispatchProps = {
  createTemplate: createTemplateAction,
  getTemplatesForOrg: getTemplatesForOrgAction,
}

export default connect<StateProps, DispatchProps, Props>(
  mstp,
  mdtp
)(withRouter(TemplateImportOverlay))
