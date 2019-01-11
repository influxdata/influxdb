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
import {QueriesState} from 'src/shared/components/TimeSeries'

interface StateProps {
  activeTab: TimeMachineTab
}

interface OwnProps {
  queriesState: QueriesState
}

type Props = StateProps & OwnProps

const TimeMachineBottom: SFC<Props> = ({activeTab, queriesState}) => {
  let tabContents

  if (activeTab === TimeMachineTab.Queries) {
    tabContents = <TimeMachineQueries queriesState={queriesState} />
  }

  if (activeTab === TimeMachineTab.Visualization) {
    tabContents = <ViewOptions />
  }

  return (
    <div className="time-machine--bottom">
      <div className="time-machine--bottom-contents">{tabContents}</div>
    </div>
  )
}

const mstp = (state: AppState) => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

export default connect<StateProps, {}, OwnProps>(
  mstp,
  null
)(TimeMachineBottom)
