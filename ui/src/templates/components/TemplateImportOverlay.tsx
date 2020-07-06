import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Copy
import {invalidJSON} from 'src/shared/copy/notifications'

// Actions
import {createTemplate as createTemplateAction} from 'src/templates/actions/thunks'
import {notify as notifyAction} from 'src/shared/actions/notifications'

// Types
import {AppState, ResourceType, Organization} from 'src/types'
import {ComponentStatus} from '@influxdata/clockface'

// Utils
import jsonlint from 'jsonlint-mod'
import {getByID} from 'src/resources/selectors'

interface State {
  status: ComponentStatus
}

interface DispatchProps {
  createTemplate: typeof createTemplateAction
  notify: typeof notifyAction
}

interface StateProps {
  org: Organization
}

type Props = DispatchProps & StateProps & RouteComponentProps<{orgID: string}>

class TemplateImportOverlay extends PureComponent<Props> {
  public state: State = {
    status: ComponentStatus.Default,
  }

  public render() {
    return (
      <ImportOverlay
        onDismissOverlay={this.onDismiss}
        resourceName="Template"
        onSubmit={this.handleImportTemplate}
        status={this.state.status}
        updateStatus={this.updateOverlayStatus}
      />
    )
  }

  private onDismiss = () => {
    const {history} = this.props

    history.goBack()
  }

  private updateOverlayStatus = (status: ComponentStatus) =>
    this.setState(() => ({status}))

  private handleImportTemplate = (importString: string) => {
    const {createTemplate, notify} = this.props

    let template
    this.updateOverlayStatus(ComponentStatus.Default)
    try {
      template = jsonlint.parse(importString)
    } catch (error) {
      this.updateOverlayStatus(ComponentStatus.Error)
      notify(invalidJSON(error.message))
      return
    }
    createTemplate(template)
    this.onDismiss()
  }
}

const mstp = (state: AppState, props: Props): StateProps => {
  const org = getByID<Organization>(
    state,
    ResourceType.Orgs,
    props.match.params.orgID
  )

  return {org}
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createTemplate: createTemplateAction,
}

export default connect<StateProps, DispatchProps, Props>(
  mstp,
  mdtp
)(withRouter(TemplateImportOverlay))
