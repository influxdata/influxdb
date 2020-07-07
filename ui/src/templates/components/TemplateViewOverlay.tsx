import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
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
import {AppState} from 'src/types'

interface OwnProps {
  match: {id: string}
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = OwnProps &
  ReduxProps &
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

const mstp = (state: AppState) => ({
  exportTemplate: state.resources.templates.exportTemplate.item,
  status: state.resources.templates.exportTemplate.status,
})

const mdtp = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(TemplateExportOverlay))
