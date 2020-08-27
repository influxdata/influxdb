import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {withRouter, RouteComponentProps} from 'react-router-dom'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

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
      <ExportOverlay
        resourceName="Template"
        resource={exportTemplate}
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
