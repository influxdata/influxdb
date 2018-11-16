// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import CSVExportButton from 'src/shared/components/CSVExportButton'

// Actions
import {setTimeRange} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {TimeRange, AppState} from 'src/types/v2'
import {QueriesState} from 'src/shared/components/TimeSeries'

interface StateProps {
  timeRange: TimeRange
}

interface DispatchProps {
  onSetTimeRange: (timeRange: TimeRange) => void
}

interface OwnProps {
  queriesState: QueriesState
}

type Props = StateProps & DispatchProps & OwnProps

class TimeMachineControls extends PureComponent<Props> {
  public render() {
    const {
      timeRange,
      onSetTimeRange,
      queriesState: {files},
    } = this.props

    return (
      <div className="time-machine-controls">
        <div className="time-machine-controls--lhs" />
        <div className="time-machine-controls--rhs">
          <CSVExportButton files={files} />
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
  const {timeRange} = getActiveTimeMachine(state)

  return {timeRange}
}

const mdtp: DispatchProps = {
  onSetTimeRange: setTimeRange,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineControls)
