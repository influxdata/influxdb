// Libraries
import React, {useState, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import FluxToolbarSearch from 'src/timeMachine/components/FluxToolbarSearch'
import {
  DapperScrollbars,
  EmptyState,
  ComponentSize,
} from '@influxdata/clockface'
import VariableItem from 'src/timeMachine/components/variableToolbar/VariableItem'

// Utils
import {extractVariablesListWithDefaults} from 'src/variables/selectors'

// Types
import {AppState, Variable} from 'src/types'

interface OwnProps {
  onClickVariable: (variableName: string) => void
}

interface StateProps {
  variables: Variable[]
}

const VariableToolbar: FunctionComponent<OwnProps & StateProps> = ({
  variables,
  onClickVariable,
}) => {
  const [searchTerm, setSearchTerm] = useState('')
  const filteredVariables = variables.filter(v => v.name.includes(searchTerm))

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
  const variables = extractVariablesListWithDefaults(state)

  return {variables}
}

export default connect<StateProps>(mstp)(VariableToolbar)
