import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import ViewOverlay from 'src/shared/components/ViewOverlay'
import {ErrorHandling} from 'src/shared/decorators/errors'

// Actions
import {
  convertToTemplate as convertToTemplateAction,
  clearExportTemplate as clearExportTemplateAction,
} from 'src/templates/actions'

// Types
import {DocumentCreate} from '@influxdata/influx'
import {AppState} from 'src/types'
import {RemoteDataState} from 'src/types'

interface OwnProps {
  templateID: string
  onDismiss: () => void
}

interface DispatchProps {
  convertToTemplate: typeof convertToTemplateAction
  clearExportTemplate: typeof clearExportTemplateAction
}

interface StateProps {
  exportTemplate: DocumentCreate
  status: RemoteDataState
}

type Props = OwnProps & StateProps & DispatchProps

@ErrorHandling
class TemplateExportOverlay extends PureComponent<Props> {
  public componentDidMount() {
    const {templateID, convertToTemplate} = this.props
    convertToTemplate(templateID)
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
      return exportTemplate.meta.name || 'Untitled Template'
    }
    return 'Untitled Template'
  }

  private onDismiss = () => {
    const {onDismiss, clearExportTemplate} = this.props

    onDismiss()
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
)(TemplateExportOverlay)
