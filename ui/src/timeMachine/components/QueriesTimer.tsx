// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Styles
import 'src/timeMachine/components/QueriesTimer.scss'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types/v2'

interface StateProps {
  status: RemoteDataState
  duration: number
}

const TimeMachineQueriesTimer: SFC<StateProps> = ({duration, status}) => {
  const visibleClass = status === RemoteDataState.Done ? 'visible' : ''

  return (
    <div className={`queries-timer ${visibleClass}`}>
      {`(${(duration / 1000).toFixed(2)}s)`}
    </div>
  )
}

const mstp = (state: AppState) => {
  const {status, fetchDuration} = getActiveTimeMachine(state).queryResults

  return {status, duration: fetchDuration}
}

export default connect<StateProps>(mstp)(TimeMachineQueriesTimer)
