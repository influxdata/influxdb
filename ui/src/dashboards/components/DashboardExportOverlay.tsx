import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/dashboards/actions/index'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions'

// Types
import {DocumentCreate} from '@influxdata/influx'
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  dashboardID: string
  onDismiss: () => void
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  dashboardTemplate: DocumentCreate
  status: RemoteDataState
}

type Props = OwnProps & StateProps & DispatchProps

class DashboardExportOverlay extends PureComponent<Props> {
  public async componentDidMount() {
    const {dashboardID, convertToTemplate} = this.props

    convertToTemplate(dashboardID)
  }

  public render() {
    const {status, dashboardTemplate} = this.props

    return (
      <ExportOverlay
        resourceName="Dashboard"
        resource={dashboardTemplate}
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
  dashboardTemplate: state.templates.exportTemplate.item,
  status: state.templates.exportTemplate.status,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(DashboardExportOverlay)
