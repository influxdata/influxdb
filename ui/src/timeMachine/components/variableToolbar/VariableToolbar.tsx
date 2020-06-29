// Libraries
import React, {useState, useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import FluxToolbarSearch from 'src/timeMachine/components/FluxToolbarSearch'
import {
  DapperScrollbars,
  EmptyState,
  ComponentSize,
} from '@influxdata/clockface'
import VariableItem from 'src/timeMachine/components/variableToolbar/VariableItem'

// Actions
import {hydrateVariables} from 'src/variables/actions/thunks'

// Utils
import {getAllVariables, sortVariablesByName} from 'src/variables/selectors'

// Types
import {AppState, Variable} from 'src/types'

interface OwnProps {
  onClickVariable: (variableName: string) => void
}

interface StateProps {
  variables: Variable[]
}

interface DispatchProps {
  hydrateVariables: typeof hydrateVariables
}

type Props = OwnProps & StateProps & DispatchProps

const VariableToolbar: FunctionComponent<Props> = ({
  variables,
  onClickVariable,
  hydrateVariables,
}) => {
  const [searchTerm, setSearchTerm] = useState('')
  const filteredVariables = variables.filter(v => v.name.includes(searchTerm))

  useEffect(() => {
    hydrateVariables()
  }, [])

  let content: JSX.Element | JSX.Element[] = (
    <EmptyState size={ComponentSize.ExtraSmall}>
      <EmptyState.Text>No variables match your search</EmptyState.Text>
    </EmptyState>
  )

  if (Boolean(filteredVariables.length)) {
    content = filteredVariables.map(v => (
      <VariableItem
        variable={v}
        key={v.id}
        onClickVariable={onClickVariable}
        testID={v.name}
      />
    ))
  }

  return (
    <>
      <FluxToolbarSearch onSearch={setSearchTerm} resourceName="Variables" />
      <DapperScrollbars className="flux-toolbar--scroll-area">
        <div className="flux-toolbar--list">{content}</div>
      </DapperScrollbars>
    </>
  )
}

const mstp = (state: AppState): StateProps => {
  const variables = getAllVariables(state)

  return {variables: sortVariablesByName(variables)}
}

const mdtp = {
  hydrateVariables: hydrateVariables,
}

export default connect<StateProps, DispatchProps>(mstp, mdtp)(VariableToolbar)
