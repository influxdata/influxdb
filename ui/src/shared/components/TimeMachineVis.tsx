// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import ViewComponent from 'src/shared/components/cells/View'

// Actions
import {setType} from 'src/shared/actions/v2/timeMachines'

// Types
import {AppState} from 'src/types/v2'
import {View, NewView, TimeRange} from 'src/types/v2'

interface StateProps {
  view: View | NewView
  timeRange: TimeRange
}

interface DispatchProps {
  onUpdateType: typeof setType
}

interface OwnProps {}

type Props = StateProps & DispatchProps & OwnProps

const TimeMachineVis: SFC<Props> = props => {
  const {view, timeRange} = props
  const noop = () => {}

  return (
    <div className="time-machine-top">
      <div className="time-machine-vis">
        <div className="graph-container">
          <ViewComponent
            view={view as View}
            onZoom={noop}
            timeRange={timeRange}
            autoRefresh={0}
            manualRefresh={0}
            onEditCell={noop}
          />
        </div>
      </div>
    </div>
  )
}

const mstp = (state: AppState) => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const timeMachine = timeMachines[activeTimeMachineID]

  return {
    view: timeMachine.view,
    timeRange: timeMachine.timeRange,
  }
}

const mdtp = {
  onUpdateType: setType,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineVis)
