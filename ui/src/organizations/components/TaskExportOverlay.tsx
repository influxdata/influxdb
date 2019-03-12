import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Utils
import {taskToTemplate} from 'src/shared/utils/resourceToTemplate'

// APIs
import {client} from 'src/utils/api'
import {ITemplate} from '@influxdata/influx'

interface State {
  taskTemplate: ITemplate
  orgID: string
}

interface Props extends WithRouterProps {
  params: {id: string}
}

class TaskExportOverlay extends PureComponent<Props, State> {
  public state: State = {taskTemplate: null, orgID: null}

  public async componentDidMount() {
    const {
      params: {id},
    } = this.props

    const task = await client.tasks.get(id)
    const taskTemplate = taskToTemplate(task)

    this.setState({taskTemplate, orgID: task.orgID})
  }

  public render() {
    const {taskTemplate, orgID} = this.state
    if (!taskTemplate) {
      return null
    }

    return (
      <ExportOverlay
        resourceName="Task"
        resource={taskTemplate}
        onDismissOverlay={this.onDismiss}
        orgID={orgID}
      />
    )
  }

  private onDismiss = () => {
    const {router} = this.props

    router.goBack()
  }
}

export default withRouter(TaskExportOverlay)
