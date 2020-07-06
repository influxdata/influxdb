import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import _ from 'lodash'

// Components
import ViewOverlay from 'src/shared/components/ViewOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  convertToTemplate as convertToTemplateAction,
  clearExportTemplate as clearExportTemplateAction,
} from 'src/templates/actions/thunks'

// Types
import {DocumentCreate} from '@influxdata/influx'
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  match: {id: string}
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  exportTemplate: DocumentCreate
  status: RemoteDataState
}

type Props = OwnProps &
  StateProps &
  DispatchProps &
  RouteComponentProps<{orgID: string; id: string}>

@ErrorHandling
class TemplateExportOverlay extends PureComponent<Props> {
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
    const {exportTemplate, status} = this.props

    return (
      <ViewOverlay
        resource={exportTemplate}
        overlayHeading={this.overlayTitle}
        onDismissOverlay={this.onDismiss}
        status={status}
      />
    )
  }

  private get overlayTitle() {
    const {exportTemplate} = this.props
    if (exportTemplate) {
      return exportTemplate.meta.name
    }
    return ''
  }

  private onDismiss = () => {
    const {history, clearExportTemplate} = this.props

    history.goBack()
    clearExportTemplate()
  }
}

const mstp = (state: AppState): StateProps => ({
  exportTemplate: state.resources.templates.exportTemplate.item,
  status: state.resources.templates.exportTemplate.status,
})

const mdtp: DispatchProps = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter(TemplateExportOverlay))
