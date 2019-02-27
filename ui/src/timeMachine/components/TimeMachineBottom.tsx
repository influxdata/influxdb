// Libraries
import React, {SFC} from 'react'

// Components
import TimeMachineQueries from 'src/timeMachine/components/Queries'

// Types
import {QueriesState} from 'src/shared/components/TimeSeries'

interface Props {
  queriesState: QueriesState
}

const TimeMachineBottom: SFC<Props> = ({queriesState}) => {
  return (
    <div className="time-machine--bottom" data-testid="time-machine--bottom">
      <div className="time-machine--bottom-contents">
        <TimeMachineQueries queriesState={queriesState} />
      </div>
    </div>
  )
}

export default TimeMachineBottom
