import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {createTaskFromTemplate as createTaskFromTemplateAction} from 'src/tasks/actions/'
import {getTasks as getTasksAction} from 'src/tasks/actions'

interface DispatchProps {
  createTaskFromTemplate: typeof createTaskFromTemplateAction
  getTasks: typeof getTasksAction
}

type Props = DispatchProps & WithRouterProps

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

  private handleImportTask = async (importString: string): Promise<void> => {
    const {createTaskFromTemplate} = this.props

    const template = JSON.parse(importString)

    await createTaskFromTemplate(template)

    this.onDismiss()
  }
}

const mdtp: DispatchProps = {
  createTaskFromTemplate: createTaskFromTemplateAction,
  getTasks: getTasksAction,
}

export default connect<{}, DispatchProps, Props>(
  null,
  mdtp
)(withRouter(TaskImportOverlay))
