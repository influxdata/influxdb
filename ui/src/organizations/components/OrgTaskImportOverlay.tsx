import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {createTaskFromTemplate as createTaskFromTemplateAction} from 'src/organizations/actions/orgView'

interface DispatchProps {
  notify: typeof notifyAction
  createTaskFromTemplate: typeof createTaskFromTemplateAction
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string}
}

type Props = DispatchProps & OwnProps

class OrgTaskImportOverlay extends PureComponent<Props> {
  public render() {
    return (
      <ImportOverlay
        onDismissOverlay={this.onDismiss}
        resourceName="Task"
        onSubmit={this.handleImportTask}
      />
    )
  }

  private onDismiss = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    // fetch tasks

    router.push(`/organizations/${orgID}/tasks`)
  }

  private handleImportTask = async (importString: string): Promise<void> => {
    const {
      params: {orgID},
    } = this.props
    const {createTaskFromTemplate} = this.props

    try {
      const template = JSON.parse(importString)

      if (_.isEmpty(template)) {
        this.onDismiss()
      }

      createTaskFromTemplate(template, orgID)

      this.onDismiss()
    } catch (error) {}
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createTaskFromTemplate: createTaskFromTemplateAction,
}

export default connect<null, DispatchProps, Props>(
  null,
  mdtp
)(withRouter(OrgTaskImportOverlay))
