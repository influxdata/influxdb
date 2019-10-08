import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import ImportOverlay from 'src/shared/components/ImportOverlay'

// Actions
import {
  createVariableFromTemplate as createVariableFromTemplateAction,
  getVariables as getVariablesAction,
} from 'src/variables/actions'

interface DispatchProps {
  createVariableFromTemplate: typeof createVariableFromTemplateAction
  getVariables: typeof getVariablesAction
}

interface OwnProps {
  onDismiss: () => void
}

type Props = OwnProps & DispatchProps

class VariableImportOverlay extends PureComponent<Props> {
  public render() {
    const {onDismiss} = this.props
    return (
      <ImportOverlay
        onDismissOverlay={onDismiss}
        resourceName="Variable"
        onSubmit={this.handleImportVariable}
      />
    )
  }

  private handleImportVariable = async (
    uploadContent: string
  ): Promise<void> => {
    const {createVariableFromTemplate, getVariables, onDismiss} = this.props

    const template = JSON.parse(uploadContent)
    await createVariableFromTemplate(template)
    getVariables()

    onDismiss()
  }
}

const mdtp: DispatchProps = {
  createVariableFromTemplate: createVariableFromTemplateAction,
  getVariables: getVariablesAction,
}

export default connect<{}, DispatchProps, OwnProps>(
  null,
  mdtp
)(VariableImportOverlay)
