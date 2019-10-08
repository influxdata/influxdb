import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {createTaskFromTemplate as createTaskFromTemplateAction} from 'src/tasks/actions/'

interface DispatchProps {
  createTaskFromTemplate: typeof createTaskFromTemplateAction
}

interface OwnProps {
  onDismiss: () => void
}

type Props = OwnProps & DispatchProps

class TaskImportOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss} = this.props

    return (
      <ImportOverlay
        onDismissOverlay={onDismiss}
        resourceName="Task"
        onSubmit={this.handleImportTask}
      />
    )
  }

  private handleImportTask = async (importString: string): Promise<void> => {
    const {createTaskFromTemplate, onDismiss} = this.props

    const template = JSON.parse(importString)

    await createTaskFromTemplate(template)

    onDismiss()
  }
}

const mdtp: DispatchProps = {
  createTaskFromTemplate: createTaskFromTemplateAction,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(TaskImportOverlay)
