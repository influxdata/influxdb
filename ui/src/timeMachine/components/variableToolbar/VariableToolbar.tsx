// Libraries
import React, {useState, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import SearchBar from 'src/timeMachine/components/SearchBar'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import VariableItem from 'src/timeMachine/components/variableToolbar/VariableItem'

// Utils
import {getAllVariables} from 'src/variables/selectors'

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

  return (
    <div className="variable-toolbar">
      <SearchBar onSearch={setSearchTerm} resourceName="Variables" />
      <FancyScrollbar style={{marginBottom: '40px'}}>
        <div className="variables-toolbar--list">
          {variables
            .filter(v => v.name.includes(searchTerm))
            .map(v => (
              <VariableItem
                variable={v}
                key={v.id}
                onClickVariable={onClickVariable}
              />
            ))}
        </div>
      </FancyScrollbar>
    </div>
  )
}

const mstp = (state: AppState): StateProps => {
  const variables = getAllVariables(state)

  return {variables}
}

export default connect<StateProps>(mstp)(VariableToolbar)
