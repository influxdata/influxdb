import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/variables/actions'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions'

// Types
import {AppState} from 'src/types'
import {DocumentCreate} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  params: {id: string; orgID: string}
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  variableTemplate: DocumentCreate
  status: RemoteDataState
  orgID: string
}

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

class OrgVariableExportOverlay extends PureComponent<Props> {
  public async componentDidMount() {
    const {
      params: {id},
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
        orgID={this.orgID}
        status={status}
      />
    )
  }

  private get orgID() {
    const orgFromExistingResource = this.props.orgID
    const orgInRoutes = this.props.params.orgID
    return orgFromExistingResource || orgInRoutes
  }

  private onDismiss = () => {
    const {router, clearExportTemplate} = this.props

    router.goBack()
    clearExportTemplate()
  }
}

const mstp = (state: AppState): StateProps => ({
  variableTemplate: state.templates.exportTemplate.item,
  status: state.templates.exportTemplate.status,
  orgID: state.templates.exportTemplate.orgID,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(OrgVariableExportOverlay))
