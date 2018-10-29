// Libraries
import React, {PureComponent, ComponentClass} from 'react'
import {connect} from 'react-redux'

// Components
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'

// Actions
import {setTimeRange} from 'src/shared/actions/v2/timeMachines'

// Types
import {TimeRange, AppState} from 'src/types/v2'

interface StateProps {
  timeRange: TimeRange
}

interface DispatchProps {
  onSetTimeRange: (timeRange: TimeRange) => void
}

interface PassedProps {}

type Props = StateProps & DispatchProps & PassedProps

class TimeMachineControls extends PureComponent<Props> {
  public render() {
    const {timeRange, onSetTimeRange} = this.props

    return (
      <div className="time-machine-controls">
        <div className="time-machine-controls--lhs" />
        <div className="time-machine-controls--rhs">
          <TimeRangeDropdown
            timeRange={timeRange}
            onSetTimeRange={onSetTimeRange}
          />
        </div>
      </div>
    )
  }
}

const mstp = (state: AppState): StateProps => {
  const {activeTimeMachineID, timeMachines} = state.timeMachines
  const {timeRange} = timeMachines[activeTimeMachineID]

  return {timeRange}
}

const mdtp: DispatchProps = {
  onSetTimeRange: setTimeRange,
}

export default connect(mstp, mdtp)(TimeMachineControls) as ComponentClass<
  PassedProps
>
