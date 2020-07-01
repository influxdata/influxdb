import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect} from 'react-redux'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/variables/actions/thunks'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions/thunks'

// Types
import {AppState} from 'src/types'
import {DocumentCreate} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  match: {id: string}
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  variableTemplate: DocumentCreate
  status: RemoteDataState
}

type Props = OwnProps &
  StateProps &
  DispatchProps &
  RouteComponentProps<{orgID: string; id: string}>

class VariableExportOverlay extends PureComponent<Props> {
  public componentDidMount() {
    const {
      match: {
        params: {id},
      },
      convertToTemplate,
    } = this.props

    convertToTemplate(id)
  }

  public render() {
    const {variableTemplate, status} = this.props

    return (
      <ExportOverlay
        resourceName="Variable"
        resource={variableTemplate}
        onDismissOverlay={this.onDismiss}
        status={status}
      />
    )
  }

  private onDismiss = () => {
    const {history, clearExportTemplate} = this.props

    history.goBack()
    clearExportTemplate()
  }
}

const mstp = (state: AppState): StateProps => ({
  variableTemplate: state.resources.templates.exportTemplate.item,
  status: state.resources.templates.exportTemplate.status,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(VariableExportOverlay))
