import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {createTaskFromTemplate as createTaskFromTemplateAction} from 'src/organizations/actions/orgView'
import {AppState, Organization} from 'src/types/v2'
import {getTasks as getTasksAction} from 'src/organizations/actions/orgView'
import {populateTasks as populateTasksAction} from 'src/tasks/actions/v2'

interface DispatchProps {
  notify: typeof notifyAction
  createTaskFromTemplate: typeof createTaskFromTemplateAction
  getTasks: typeof getTasksAction
  populateTasks: typeof populateTasksAction
}

interface StateProps {
  orgs: Organization[]
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string}
}

type Props = DispatchProps & StateProps & OwnProps

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
    const {router} = this.props

    router.goBack()
  }

  private handleImportTask = async (importString: string): Promise<void> => {
    const {
      params: {orgID},
      orgs,
      createTaskFromTemplate,
      getTasks,
      populateTasks,
    } = this.props

    try {
      const template = JSON.parse(importString)

      if (_.isEmpty(template)) {
        this.onDismiss()
      }

      await createTaskFromTemplate(template, orgID || orgs[0].id)

      if (orgID) {
        // import overlay is in org view
        getTasks(orgID)
      } else {
        // import overlay is in tasks view
        populateTasks()
      }

      this.onDismiss()
    } catch (error) {}
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createTaskFromTemplate: createTaskFromTemplateAction,
  getTasks: getTasksAction,
  populateTasks: populateTasksAction,
}

const mstp = (state: AppState): StateProps => {
  const {orgs} = state
  return {orgs}
}

export default connect<StateProps, DispatchProps, Props>(
  mstp,
  mdtp
)(withRouter(OrgTaskImportOverlay))
