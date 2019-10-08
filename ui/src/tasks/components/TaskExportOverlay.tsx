import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/tasks/actions'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions'

// Types
import {AppState} from 'src/types'
import {DocumentCreate} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  taskID: string
  onDismiss: () => void
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  taskTemplate: DocumentCreate
  status: RemoteDataState
}

type Props = OwnProps & StateProps & DispatchProps

class TaskExportOverlay extends PureComponent<Props> {
  public async componentDidMount() {
    const {taskID, convertToTemplate} = this.props

    convertToTemplate(taskID)
  }

  public render() {
    const {taskTemplate, status} = this.props

    return (
      <ExportOverlay
        resourceName="Task"
        resource={taskTemplate}
        onDismissOverlay={this.onDismiss}
        status={status}
      />
    )
  }

  private onDismiss = () => {
    const {onDismiss, clearExportTemplate} = this.props

    onDismiss()
    clearExportTemplate()
  }
}

const mstp = (state: AppState): StateProps => ({
  taskTemplate: state.templates.exportTemplate.item,
  status: state.templates.exportTemplate.status,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TaskExportOverlay)
