// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

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
import {
  AppState,
  VariableArgumentType,
  QueryArguments,
  CSVArguments,
  MapArguments,
  Variable,
} from 'src/types'

interface ComponentProps {
  onHideOverlay?: () => void
  initialScript?: string
  submitButtonText?: string
}

interface DispatchProps {
  onCreateVariable: (
    variable: Pick<Variable, 'name' | 'arguments' | 'selected'>
  ) => void
  onNameUpdate: typeof updateName
  onTypeUpdate: typeof updateType
  onQueryUpdate: typeof updateQuery
  onMapUpdate: typeof updateMap
  onConstantUpdate: typeof updateConstant
  onEditorClose: typeof clearEditor
}

interface StateProps {
  variables: Variable[]
  name: string
  variableType: VariableArgumentType
  query: QueryArguments
  map: MapArguments
  constant: CSVArguments
}

type Props = ComponentProps & DispatchProps & StateProps

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

const mstp = (state: AppState): StateProps => {
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

const mdtp: DispatchProps = {
  onNameUpdate: updateName,
  onTypeUpdate: updateType,
  onQueryUpdate: updateQuery,
  onMapUpdate: updateMap,
  onConstantUpdate: updateConstant,
  onEditorClose: clearEditor,
  onCreateVariable: createVariable,
}

export {Props}
export {VariableFormContext}
export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(VariableFormContext)
