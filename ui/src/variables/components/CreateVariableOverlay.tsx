// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'
import {withRouter, WithRouterProps} from 'react-router'

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
  createVariable,
  updateName,
  updateType,
  updateQuery,
  updateMap,
  updateConstant,
  clearEditor,
} from 'src/variables/actions'

// Components
import {Overlay} from '@influxdata/clockface'
import VariableForm from 'src/variables/components/VariableForm'
import GetResources, {ResourceType} from 'src/shared/components/GetResources'

// Types
import {AppState, VariableArguments, VariableArgumentType} from 'src/types'
import {IVariable as Variable} from '@influxdata/influx'

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
  variables: Variable[]
  name: string
  variableType: VariableArgumentType
  query: VariableArguments
  map: VariableArguments
  constant: VariableArguments
}

type Props = DispatchProps & WithRouterProps & StateProps

class CreateVariableOverlay extends PureComponent<Props> {
  public render() {
    const {
      onCreateVariable,
      variables,
      name,
      onNameUpdate,
      onTypeUpdate,
      onQueryUpdate,
      onMapUpdate,
      onConstantUpdate,
      variableType,
      query,
      map,
      constant,
    } = this.props

    return (
      <GetResources resource={ResourceType.Variables}>
        <Overlay visible={true}>
          <Overlay.Container maxWidth={1000}>
            <Overlay.Header
              title="Create Variable"
              onDismiss={this.handleHideOverlay}
            />
            <Overlay.Body>
              <VariableForm
                name={name}
                variableType={variableType}
                query={query}
                map={map}
                constant={constant}
                variables={variables}
                onCreateVariable={onCreateVariable}
                onNameUpdate={onNameUpdate}
                onTypeUpdate={onTypeUpdate}
                onQueryUpdate={onQueryUpdate}
                onMapUpdate={onMapUpdate}
                onConstantUpdate={onConstantUpdate}
                onHideOverlay={this.handleHideOverlay}
              />
            </Overlay.Body>
          </Overlay.Container>
        </Overlay>
      </GetResources>
    )
  }

  private handleHideOverlay = () => {
    const {
      router,
      params: {orgID},
      onEditorClose,
    } = this.props

    onEditorClose()
    router.push(`/orgs/${orgID}/settings/variables`)
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
  onCreateVariable: createVariable,
  onNameUpdate: updateName,
  onTypeUpdate: updateType,
  onQueryUpdate: updateQuery,
  onMapUpdate: updateMap,
  onConstantUpdate: updateConstant,
  onEditorClose: clearEditor,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(withRouter<{}>(CreateVariableOverlay))
