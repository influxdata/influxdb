// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import TimeMachineControls from 'src/shared/components/TimeMachineControls'
import Threesizer from 'src/shared/components/threesizer/Threesizer'
import TimeMachineBottom from 'src/shared/components/TimeMachineBottom'
import TimeMachineVis from 'src/shared/components/TimeMachineVis'
import TimeSeries from 'src/shared/components/TimeSeries'

// Utils
import {getActiveSource} from 'src/sources/selectors'
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Constants
import {HANDLE_HORIZONTAL} from 'src/shared/constants'

// Types
import {AppState, DashboardQuery} from 'src/types/v2'

interface StateProps {
  queryLink: string
  queries: DashboardQuery[]
}

const TimeMachine: SFC<StateProps> = props => {
  const {queryLink, queries} = props

  return (
    <div className="time-machine">
      <TimeSeries link={queryLink} queries={queries}>
        {queriesState => {
          const divisions = [
            {
              handleDisplay: 'none',
              render: () => <TimeMachineVis queriesState={queriesState} />,
              headerOrientation: HANDLE_HORIZONTAL,
              size: 0.33,
            },
            {
              handlePixels: 12,
              render: () => <TimeMachineBottom queryStatus={queriesState.loading} />,
              headerOrientation: HANDLE_HORIZONTAL,
              size: 0.67,
            },
          ]

          return (
            <>
              <TimeMachineControls queriesState={queriesState} />
              <div className="time-machine-container">
                <Threesizer
                  orientation={HANDLE_HORIZONTAL}
                  divisions={divisions}
                />
              </div>
            </>
          )
        }}
      </TimeSeries>
    </div>
  )
}

const mstp = (state: AppState) => {
  const timeMachine = getActiveTimeMachine(state)
  const queries = get(timeMachine, 'view.properties.queries', [])
  const queryLink = getActiveSource(state).links.query

  return {queryLink, queries}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TimeMachine)
