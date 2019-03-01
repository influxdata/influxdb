import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// APIs
import {client} from 'src/utils/api'
import {Task} from '@influxdata/influx'

interface State {
  task: Task
}

interface Props extends WithRouterProps {
  params: {id: string; orgID: string}
}

class OrgTaskExportOverlay extends PureComponent<Props, State> {
  public state = {task: null}
  public async componentDidMount() {
    const {
      params: {id},
    } = this.props

    const task = await client.tasks.get(id)

    this.setState({task})
  }

  public render() {
    const {task} = this.state
    if (!task) {
      return null
    }
    return (
      <ExportOverlay
        resourceName="Task"
        resource={task}
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
