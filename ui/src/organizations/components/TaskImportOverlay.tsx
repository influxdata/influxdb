import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {notify as notifyAction} from 'src/shared/actions/notifications'
import {createTaskFromTemplate as createTaskFromTemplateAction} from 'src/organizations/actions/orgView'
import {getTasks as getTasksAction} from 'src/organizations/actions/orgView'
import {populateTasks as populateTasksAction} from 'src/tasks/actions/v2'
import {
  importTaskFailed,
  importTaskSucceeded,
} from 'src/shared/copy/notifications'

interface DispatchProps {
  notify: typeof notifyAction
  createTaskFromTemplate: typeof createTaskFromTemplateAction
  getTasks: typeof getTasksAction
  populateTasks: typeof populateTasksAction
}

interface OwnProps extends WithRouterProps {
  params: {orgID: string}
}

type Props = DispatchProps & OwnProps

class TaskImportOverlay extends PureComponent<Props> {
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

  private handleImportTask = async (
    importString: string,
    orgID: string
  ): Promise<void> => {
    const {createTaskFromTemplate, getTasks, populateTasks, notify} = this.props

    try {
      const template = JSON.parse(importString)

      await createTaskFromTemplate(template, orgID)

      if (get(this.props.params, 'orgID', '')) {
        // import overlay is in org view
        getTasks(orgID)
      } else {
        // import overlay is in tasks view
        populateTasks()
      }

      this.onDismiss()
      notify(importTaskSucceeded())
    } catch (error) {
      notify(importTaskFailed(error))
    }
  }
}

const mdtp: DispatchProps = {
  notify: notifyAction,
  createTaskFromTemplate: createTaskFromTemplateAction,
  getTasks: getTasksAction,
  populateTasks: populateTasksAction,
}

export default connect<{}, DispatchProps, Props>(
  null,
  mdtp
)(withRouter(TaskImportOverlay))
