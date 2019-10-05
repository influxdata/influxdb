// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'

// Components
import VariableForm from 'src/variables/components/VariableForm'

// Utils
import {
  createVariable,
  updateName,
  updateType,
  updateQuery,
  updateMap,
  updateConstant,
  clearEditor,
} from 'src/variables/actions'
import {
  extractVariablesList,
  extractVariableEditorName,
  extractVariableEditorType,
  extractVariableEditorQuery,
  extractVariableEditorMap,
  extractVariableEditorConstant,
} from 'src/variables/selectors'

// Types
import {AppState, VariableArguments, VariableArgumentType} from 'src/types'
import {getActiveQuery} from 'src/timeMachine/selectors'
import {IVariable as Variable} from '@influxdata/influx'

interface OwnProps {
  onHideOverlay: () => void
}

interface DispatchProps {
  onCreateVariable: typeof createVariable
  onNameUpdate: typeof updateName
  onTypeUpdate: typeof updateType
  onQueryUpdate: typeof updateQuery
  onMapUpdate: typeof updateMap
  onConstantUpdate: typeof updateConstant
  onEditorClose: typeof clearEditor
}

interface StateProps {
  initialScript?: string
  variables: Variable[]
  name: string
  variableType: VariableArgumentType
  query: VariableArguments
  map: VariableArguments
  constant: VariableArguments
}

type Props = StateProps & DispatchProps & OwnProps

class SaveAsVariable extends PureComponent<Props & WithRouterProps> {
  render() {
    const {
      onCreateVariable,
      onNameUpdate,
      onTypeUpdate,
      onQueryUpdate,
      onMapUpdate,
      onConstantUpdate,
      name,
      initialScript,
      variables,
      variableType,
      query,
      map,
      constant,
    } = this.props

    return (
      <VariableForm
        name={name}
        variableType={variableType}
        query={query}
        map={map}
        constant={constant}
        variables={variables}
        initialScript={initialScript}
        onHideOverlay={this.handleHideOverlay}
        onCreateVariable={onCreateVariable}
        onNameUpdate={onNameUpdate}
        onTypeUpdate={onTypeUpdate}
        onQueryUpdate={onQueryUpdate}
        onMapUpdate={onMapUpdate}
        onConstantUpdate={onConstantUpdate}
      />
    )
  }

  private handleHideOverlay = () => {
    const {onEditorClose, onHideOverlay} = this.props

    onEditorClose()
    onHideOverlay()
  }
}

const mstp = (state: AppState): StateProps => {
  const activeQuery = getActiveQuery(state),
    variables = extractVariablesList(state),
    name = extractVariableEditorName(state),
    variableType = extractVariableEditorType(state),
    query = extractVariableEditorQuery(state),
    map = extractVariableEditorMap(state),
    constant = extractVariableEditorConstant(state)

  return {
    initialScript: activeQuery.text,
    variables,
    name,
    variableType,
    query,
    map,
    constant,
  }
}

const mdtp = {
  onCreateVariable: createVariable,
  onNameUpdate: updateName,
  onTypeUpdate: updateType,
  onQueryUpdate: updateQuery,
  onMapUpdate: updateMap,
  onConstantUpdate: updateConstant,
  onEditorClose: clearEditor,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(withRouter<Props>(SaveAsVariable))
