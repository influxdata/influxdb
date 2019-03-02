import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Utils
import {taskToTemplate} from 'src/shared/utils/resourceToTemplate'

// APIs
import {client} from 'src/utils/api'
import {Task} from 'src/types/v2'

interface State {
  taskTemplate: Record<string, any>
}

interface Props extends WithRouterProps {
  params: {id: string; orgID: string}
}

class OrgTaskExportOverlay extends PureComponent<Props, State> {
  public state = {taskTemplate: null}

  public async componentDidMount() {
    const {
      params: {id},
    } = this.props

    const task = (await client.tasks.get(id)) as Task
    const taskTemplate = taskToTemplate(task)

    this.setState({taskTemplate})
  }

  public render() {
    const {taskTemplate} = this.state
    if (!taskTemplate) {
      return null
    }
    return (
      <ExportOverlay
        resourceName="Task"
        resource={taskTemplate}
        onDismissOverlay={this.onDismiss}
      />
    )
  }

  private onDismiss = () => {
    const {
      router,
      params: {orgID},
    } = this.props

    router.push(`/organizations/${orgID}/tasks`)
  }
}

export default withRouter(OrgTaskExportOverlay)
