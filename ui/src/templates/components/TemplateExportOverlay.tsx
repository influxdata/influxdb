import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {
  convertToTemplate as convertToTemplateAction,
  clearExportTemplate as clearExportTemplateAction,
} from 'src/templates/actions'

// Types
import {DocumentCreate} from '@influxdata/influx'
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  params: {id: string; orgID: string}
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  exportTemplate: DocumentCreate
  status: RemoteDataState
}

type Props = OwnProps & StateProps & DispatchProps & WithRouterProps

class TemplateExportOverlay extends PureComponent<Props> {
  public componentDidMount() {
    const {
      params: {id},
      convertToTemplate,
    } = this.props
    convertToTemplate(id)
  }

  public render() {
    const {exportTemplate, status} = this.props
    const {
      params: {orgID},
    } = this.props

    return (
      <ExportOverlay
        resourceName="Template"
        resource={exportTemplate}
        onDismissOverlay={this.onDismiss}
        orgID={orgID}
        status={status}
      />
    )
  }

  private onDismiss = () => {
    const {router, clearExportTemplate} = this.props

    router.goBack()
    clearExportTemplate()
  }
}

const mstp = (state: AppState): StateProps => ({
  exportTemplate: state.templates.exportTemplate.item,
  status: state.templates.exportTemplate.status,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(TemplateExportOverlay))
