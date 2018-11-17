// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineQueryEditor from 'src/shared/components/TimeMachineQueryEditor'
import ViewOptions from 'src/shared/components/view_options/ViewOptions'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {TimeMachineTab} from 'src/types/v2/timeMachine'
import {AppState} from 'src/types/v2'

interface StateProps {
  activeTab: TimeMachineTab
}

const TimeMachineBottom: SFC<StateProps> = props => {
  const {activeTab} = props

  if (activeTab === TimeMachineTab.Queries) {
    return <TimeMachineQueryEditor />
  }

  if (activeTab === TimeMachineTab.Visualization) {
    return (
      <div className="time-machine-customization">
        <ViewOptions />
      </div>
    )
  }

  return null
}

const mstp = (state: AppState) => {
  const {activeTab} = getActiveTimeMachine(state)

  return {activeTab}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(TimeMachineBottom)
