import React, {PureComponent} from 'react'
import {withRouter, RouteComponentProps} from 'react-router-dom'
import {connect, ConnectedProps} from 'react-redux'

// Components
import ExportOverlay from 'src/shared/components/ExportOverlay'

// Actions
import {convertToTemplate as convertToTemplateAction} from 'src/variables/actions/thunks'
import {clearExportTemplate as clearExportTemplateAction} from 'src/templates/actions/thunks'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = RouteComponentProps<{orgID: string; id: string}> & ReduxProps

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

const mstp = (state: AppState) => ({
  variableTemplate: state.resources.templates.exportTemplate.item,
  status: state.resources.templates.exportTemplate.status,
})

const mdtp = {
  convertToTemplate: convertToTemplateAction,
  clearExportTemplate: clearExportTemplateAction,
}

const connector = connect(mstp, mdtp)

export default connector(withRouter(VariableExportOverlay))
