// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import classnames from 'classnames'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {RemoteDataState} from 'src/types'
import {AppState} from 'src/types'

interface StateProps {
  status: RemoteDataState
  duration: number
}

const TimeMachineQueriesTimer: SFC<StateProps> = ({duration, status}) => {
  const queriesTimerClass = classnames('query-tab--timer', {
    'query-tab--timer__visible': status === RemoteDataState.Done,
  })

  return (
    <div className={queriesTimerClass}>
      {`(${(duration / 1000).toFixed(2)}s)`}
    </div>
  )
}

const mstp = (state: AppState) => {
  const {status, fetchDuration} = getActiveTimeMachine(state).queryResults

  return {status, duration: fetchDuration}
}

export default connect<StateProps>(mstp)(TimeMachineQueriesTimer)
