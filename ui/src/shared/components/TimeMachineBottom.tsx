// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import ViewTypeSelector from 'src/shared/components/ViewTypeSelector'
import TimeMachineQueryEditor from 'src/shared/components/TimeMachineQueryEditor'

// Actions
import {setType} from 'src/shared/actions/v2/timeMachines'

// Types
import {AppState, View, NewView} from 'src/types/v2'
import {TimeMachineTab} from 'src/types/v2/timeMachine'

interface StateProps {
  view: View | NewView
}

interface DispatchProps {
  onUpdateType: typeof setType
}

interface OwnProps {
  activeTab: TimeMachineTab
}

type Props = StateProps & DispatchProps & OwnProps

const TimeMachineBottom: SFC<Props> = props => {
  const {view, onUpdateType, activeTab} = props

  if (activeTab === TimeMachineTab.Queries) {
    return <TimeMachineQueryEditor />
  }

  if (activeTab === TimeMachineTab.Visualization) {
    return (
      <div className="time-machine-customization">
        <ViewTypeSelector
          type={view.properties.type}
          onUpdateType={onUpdateType}
        />
      </div>
    )
  }

  return null
}

const mstp = (state: AppState) => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return {view: timeMachine.view}
}

const mdtp = {
  onUpdateType: setType,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineBottom)
