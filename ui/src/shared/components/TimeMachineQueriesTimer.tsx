// Libraries
import React, {SFC} from 'react'

// Styles
import 'src/shared/components/TimeMachineQueriesTimer.scss'

// Types
import {RemoteDataState} from 'src/types'

interface Props {
  status: RemoteDataState
  duration: number
}

const TimeMachineQueriesTimer: SFC<Props> = ({duration, status}) => {
  const visibleClass = status === RemoteDataState.Done ? 'visible' : ''

  return (
    <div className={`time-machine-queries-timer ${visibleClass}`}>
      {`(${(duration / 1000).toFixed(2)}s)`}
    </div>
  )
}

export default TimeMachineQueriesTimer
