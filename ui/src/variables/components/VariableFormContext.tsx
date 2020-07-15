// Libraries
import React, {PureComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Utils
import {
  getVariables,
  extractVariableEditorName,
  extractVariableEditorType,
  extractVariableEditorQuery,
  extractVariableEditorMap,
  extractVariableEditorConstant,
} from 'src/variables/selectors'

// Actions
import {
  updateName,
  updateType,
  updateQuery,
  updateMap,
  updateConstant,
  clearEditor,
} from 'src/variables/actions/creators'
import {createVariable} from 'src/variables/actions/thunks'

// Component
import VariableForm from 'src/variables/components/VariableForm'

// Types
import {AppState} from 'src/types'

interface ComponentProps {
  onHideOverlay?: () => void
  initialScript?: string
  submitButtonText?: string
}

type ReduxProps = ConnectedProps<typeof connector>
type Props = ComponentProps & ReduxProps

class VariableFormContext extends PureComponent<Props> {
  render() {
    const props = {
      ...this.props,
      onHideOverlay: this.handleHideOverlay,
    }

    return <VariableForm {...props} />
  }

  private handleHideOverlay = () => {
    const {onHideOverlay, onEditorClose} = this.props

    onEditorClose()
    onHideOverlay()
  }
}

const mstp = (state: AppState) => {
  const variables = getVariables(state),
    name = extractVariableEditorName(state),
    variableType = extractVariableEditorType(state),
    query = extractVariableEditorQuery(state),
    map = extractVariableEditorMap(state),
    constant = extractVariableEditorConstant(state)

  return {
    variables,
    name,
    variableType,
    query,
    map,
    constant,
  }
}

const mdtp = {
  onNameUpdate: updateName,
  onTypeUpdate: updateType,
  onQueryUpdate: updateQuery,
  onMapUpdate: updateMap,
  onConstantUpdate: updateConstant,
  onEditorClose: clearEditor,
  onCreateVariable: createVariable,
}

const connector = connect(mstp, mdtp)

export {Props}
export {VariableFormContext}
export default connector(VariableFormContext)
