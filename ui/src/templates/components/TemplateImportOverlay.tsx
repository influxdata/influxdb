import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {
  createTemplate as createTemplateAction,
  getTemplates as getTemplatesAction,
} from 'src/templates/actions'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState, Organization} from 'src/types'

interface DispatchProps {
  createTemplate: typeof createTemplateAction
  getTemplates: typeof getTemplatesAction
  notify: typeof notifyAction
}

interface StateProps {
  org: Organization
}

interface OwnProps {
  onDismiss: () => void
}

type Props = OwnProps & DispatchProps & StateProps

class TemplateImportOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss} = this.props
    return (
      <ImportOverlay
        onDismissOverlay={onDismiss}
        resourceName="Template"
        onSubmit={this.handleImportTemplate}
      />
    )
  }

  private handleImportTemplate = async (
    importString: string
  ): Promise<void> => {
    const {createTemplate, getTemplates, onDismiss} = this.props

    const template = JSON.parse(importString)
    await createTemplate(template)

    getTemplates()

    onDismiss()
  }
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {org},
  } = state

  return {org}
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createTemplate: createTemplateAction,
  getTemplates: getTemplatesAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TemplateImportOverlay)
