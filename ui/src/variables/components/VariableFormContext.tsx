// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Utils
import {
  extractVariablesList,
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
  createVariable,
} from 'src/variables/actions'

// Component
import VariableForm from 'src/variables/components/VariableForm'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import {
  AppState,
  VariableArguments,
  VariableArgumentType,
} from 'src/types'

interface ComponentProps {
  onHideOverlay?: () => void
  initialScript?: string
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
  query: VariableArguments
  map: VariableArguments
  constant: VariableArguments
}

type Props = ComponentProps & DispatchProps & StateProps

class VariableFormContext extends PureComponent<Props> {
  render() {
    const {onHideOverlay, onEditorClose} = this.props
    const hideOverlay = () => {
      onEditorClose()
      onHideOverlay()
    }
    const props = {
      onHideOverlay: hideOverlay,
      ...this.props
    }
    return (
      <VariableForm {...props}/>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const variables = extractVariablesList(state),
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
