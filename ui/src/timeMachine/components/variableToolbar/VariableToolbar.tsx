// Libraries
import React, {useState, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import SearchBar from 'src/timeMachine/components/SearchBar'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import VariableItem from 'src/timeMachine/components/variableToolbar/VariableItem'

// Utils
import {getVariablesForOrg} from 'src/variables/selectors'

// Types
import {IVariable as Variable} from '@influxdata/influx'
import {AppState} from 'src/types'

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
      <FancyScrollbar>
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

const mstp = (state: AppState) => {
  const org = state.orgs.org
  const variables = getVariablesForOrg(state, org.id)
  const {status: variablesStatus} = state.variables

  return {variables, variablesStatus}
}

export default connect<StateProps>(mstp)(VariableToolbar)
