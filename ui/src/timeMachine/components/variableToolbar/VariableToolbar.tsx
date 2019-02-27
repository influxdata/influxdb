// Libraries
import React, {useState, useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import SearchBar from 'src/timeMachine/components/SearchBar'
import FancyScrollbar from 'src/shared/components/fancy_scrollbar/FancyScrollbar'
import VariableItem from 'src/timeMachine/components/variableToolbar/VariableItem'
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Utils
import {getVariablesForOrg} from 'src/variables/selectors'
import {getActiveOrg} from 'src/organizations/selectors'

// Actions
import {getVariables} from 'src/variables/actions'

// Styles
import 'src/timeMachine/components/variableToolbar/VariableToolbar.scss'

// Types
import {Variable} from '@influxdata/influx'
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'

interface StateProps {
  variables: Variable[]
  variablesStatus: RemoteDataState
}

interface DispatchProps {
  onGetVariables: typeof getVariables
}

type Props = StateProps & DispatchProps

const VariableToolbar: FunctionComponent<Props> = ({
  variables,
  variablesStatus,
  onGetVariables,
}) => {
  const [searchTerm, setSearchTerm] = useState('')

  useEffect(() => {
    if (variablesStatus === RemoteDataState.NotStarted) {
      onGetVariables()
    }
  }, [])

  return (
    <SpinnerContainer
      loading={variablesStatus}
      spinnerComponent={<TechnoSpinner />}
    >
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
    </SpinnerContainer>
  )
}

const mstp = (state: AppState) => {
  const org = getActiveOrg(state)
  const variables = getVariablesForOrg(state, org.id)
  const {status: variablesStatus} = state.variables

  return {variables, variablesStatus}
}

const mdtp = {
  onGetVariables: getVariables,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(VariableToolbar)
