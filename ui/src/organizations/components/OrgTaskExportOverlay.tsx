import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Utils
import {taskToTemplate} from 'src/shared/utils/resourceToTemplate'

// APIs
import {client} from 'src/utils/api'
import {ITemplate} from '@influxdata/influx'
import {Task} from 'src/types/v2'

interface State {
  taskTemplate: ITemplate
}

interface Props extends WithRouterProps {
  params: {id: string; orgID: string}
}

class OrgTaskExportOverlay extends PureComponent<Props, State> {
  public state: State = {taskTemplate: null}

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
    const {router} = this.props

    router.goBack()
  }
}

export default withRouter(OrgTaskExportOverlay)
