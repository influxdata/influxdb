// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineQueries from 'src/shared/components/TimeMachineQueries'
import ViewOptions from 'src/shared/components/view_options/ViewOptions'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'

interface StateProps {
  activeTab: TimeMachineTab
}

interface OwnProps {
  queryStatus: RemoteDataState
}

type Props = StateProps & OwnProps

const TimeMachineBottom: SFC<Props> = ({activeTab, queryStatus}) => {
  if (activeTab === TimeMachineTab.Queries) {
    return <TimeMachineQueries queryStatus={queryStatus} />
  }

  if (activeTab === TimeMachineTab.Visualization) {
    return <ViewOptions />
  }

  return null
}

const mstp = (state: AppState) => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(TimeMachineBottom)
