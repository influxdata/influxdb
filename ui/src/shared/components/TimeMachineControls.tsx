// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import CSVExportButton from 'src/shared/components/CSVExportButton'
import {
  SlideToggle,
  ComponentSize,
  ComponentSpacer,
  Alignment,
} from 'src/clockface'
import TimeMachineRefreshDropdown from 'src/shared/components/TimeMachineRefreshDropdown'

// Actions
import {
  setTimeRange,
  setIsViewingRawData,
} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {TimeRange, AppState} from 'src/types/v2'
import {QueriesState} from 'src/shared/components/TimeSeries'

interface StateProps {
  timeRange: TimeRange
  isViewingRawData: boolean
}

interface DispatchProps {
  onSetTimeRange: (timeRange: TimeRange) => void
  onSetIsViewingRawData: typeof setIsViewingRawData
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
      isViewingRawData,
      queriesState: {files},
    } = this.props

    return (
      <div className="time-machine--controls">
        <ComponentSpacer align={Alignment.Right}>
          <SlideToggle.Label text="View Raw Data" />
          <SlideToggle
            active={isViewingRawData}
            onChange={this.handleToggleIsViewingRawData}
            size={ComponentSize.ExtraSmall}
          />
          <CSVExportButton files={files} />
          <TimeMachineRefreshDropdown />
          <TimeRangeDropdown
            timeRange={timeRange}
            onSetTimeRange={onSetTimeRange}
          />
        </ComponentSpacer>
      </div>
    )
  }

  private handleToggleIsViewingRawData = () => {
    const {isViewingRawData, onSetIsViewingRawData} = this.props

    onSetIsViewingRawData(!isViewingRawData)
  }
}

const mstp = (state: AppState): StateProps => {
  const {timeRange, isViewingRawData} = getActiveTimeMachine(state)

  return {timeRange, isViewingRawData}
}

const mdtp: DispatchProps = {
  onSetTimeRange: setTimeRange,
  onSetIsViewingRawData: setIsViewingRawData,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineControls)
