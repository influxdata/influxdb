// Libraries
import React, {useState, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import SearchBar from 'src/timeMachine/components/SearchBar'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import VariableItem from 'src/timeMachine/components/variableToolbar/VariableItem'

// Utils
import {getVariablesForOrg} from 'src/variables/selectors'
import {getActiveOrg} from 'src/organizations/selectors'

// Styles
import 'src/timeMachine/components/variableToolbar/VariableToolbar.scss'

// Types
import {Variable} from '@influxdata/influx'
import {AppState} from 'src/types/v2'

interface StateProps {
  variables: Variable[]
}

const VariableToolbar: FunctionComponent<StateProps> = ({variables}) => {
  const [searchTerm, setSearchTerm] = useState('')

  return (
    <div className="variable-toolbar">
      <SearchBar onSearch={setSearchTerm} resourceName={'Variables'} />
      <FancyScrollbar>
        <div className="variables-toolbar--list">
          {variables
            .filter(v => v.name.includes(searchTerm))
            .map(v => (
              <VariableItem variable={v} key={v.id} />
            ))}
        </div>
      </FancyScrollbar>
    </div>
  )
}

const mstp = (state: AppState) => {
  const org = getActiveOrg(state)
  const variables = getVariablesForOrg(state, org.id)
  const {status: variablesStatus} = state.variables

  return {variables, variablesStatus}
}

export default connect<StateProps>(mstp)(VariableToolbar)
