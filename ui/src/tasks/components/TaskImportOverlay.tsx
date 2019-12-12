import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Copy
import {invalidJSON} from 'src/shared/copy/notifications'

// Actions
import {createTaskFromTemplate as createTaskFromTemplateAction} from 'src/tasks/actions/'
import {notify as notifyAction} from 'src/shared/actions/notifications'

interface DispatchProps {
  createTaskFromTemplate: typeof createTaskFromTemplateAction
  notify: typeof notifyAction
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

  private handleImportTask = (importString: string) => {
    const {createTaskFromTemplate, notify} = this.props

    let template
    try {
      template = JSON.parse(importString)
    } catch (error) {
      notify(invalidJSON(error.message))
      return
    }

    createTaskFromTemplate(template)
    this.onDismiss()
  }
}

const mdtp: DispatchProps = {
  createTaskFromTemplate: createTaskFromTemplateAction,
  notify: notifyAction,
}

export default connect<{}, DispatchProps, Props>(
  null,
  mdtp
)(withRouter(TaskImportOverlay))
