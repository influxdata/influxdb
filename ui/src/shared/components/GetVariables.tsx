// Libraries
import React, {useEffect, FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import {SpinnerContainer, TechnoSpinner} from '@influxdata/clockface'

// Actions
import {getVariables} from 'src/variables/actions'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'

interface StateProps {
  variablesStatus: RemoteDataState
}

interface DispatchProps {
  onGetVariables: typeof getVariables
}

type Props = StateProps & DispatchProps

const GetVariables: FunctionComponent<Props> = ({
  variablesStatus,
  onGetVariables,
  children,
}) => {
  useEffect(() => {
    if (variablesStatus === RemoteDataState.NotStarted) {
      onGetVariables()
    }
  }, [variablesStatus])

  return (
    <SpinnerContainer
      loading={variablesStatus}
      spinnerComponent={<TechnoSpinner />}
    >
      <>{children}</>
    </SpinnerContainer>
  )
}

const mstp = (state: AppState) => {
  return {variablesStatus: state.variables.status}
}

const mdtp = {
  onGetVariables: getVariables,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(GetVariables)
