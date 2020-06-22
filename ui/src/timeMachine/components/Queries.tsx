// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineFluxEditor from 'src/timeMachine/components/TimeMachineFluxEditor'
import CSVExportButton from 'src/shared/components/CSVExportButton'
import TimeMachineQueriesSwitcher from 'src/timeMachine/components/QueriesSwitcher'
import TimeMachineRefreshDropdown from 'src/timeMachine/components/RefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import TimeMachineQueryBuilder from 'src/timeMachine/components/QueryBuilder'
import SubmitQueryButton from 'src/timeMachine/components/SubmitQueryButton'
import RawDataToggle from 'src/timeMachine/components/RawDataToggle'
import QueryTabs from 'src/timeMachine/components/QueryTabs'
import EditorShortcutsToolTip from 'src/timeMachine/components/EditorShortcutsTooltip'
import {
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'

// Actions
import {setAutoRefresh} from 'src/timeMachine/actions'
import {setTimeRange} from 'src/timeMachine/actions'

// Utils
import {
  getActiveTimeMachine,
  getIsInCheckOverlay,
  getActiveQuery,
} from 'src/timeMachine/selectors'
import {getTimeRange} from 'src/dashboards/selectors'

// Types
import {
  AppState,
  DashboardQuery,
  TimeRange,
  AutoRefresh,
  AutoRefreshStatus,
} from 'src/types'

interface StateProps {
  activeQuery: DashboardQuery
  timeRange: TimeRange
  autoRefresh: AutoRefresh
  isInCheckOverlay: boolean
}

interface DispatchProps {
  onSetTimeRange: typeof setTimeRange
  onSetAutoRefresh: typeof setAutoRefresh
}

type Props = StateProps & DispatchProps

class TimeMachineQueries extends PureComponent<Props> {
  public render() {
    const {timeRange, isInCheckOverlay, activeQuery} = this.props

    return (
      <div className="time-machine-queries">
        <div className="time-machine-queries--controls">
          <QueryTabs />
          <FlexBox
            direction={FlexDirection.Row}
            justifyContent={JustifyContent.FlexEnd}
            margin={ComponentSize.Small}
            className="time-machine-queries--buttons"
          >
            {activeQuery.editMode === 'advanced' && <EditorShortcutsToolTip />}
            <RawDataToggle />
            {!isInCheckOverlay && (
              <>
                <CSVExportButton />
                <TimeMachineRefreshDropdown />
                <TimeRangeDropdown
                  timeRange={timeRange}
                  onSetTimeRange={this.handleSetTimeRange}
                />
                <TimeMachineQueriesSwitcher />
              </>
            )}
            <SubmitQueryButton />
          </FlexBox>
        </div>
        <div className="time-machine-queries--body">{this.queryEditor}</div>
      </div>
    )
  }

  private handleSetTimeRange = (timeRange: TimeRange) => {
    const {autoRefresh, onSetAutoRefresh, onSetTimeRange} = this.props

    onSetTimeRange(timeRange)

    if (timeRange.type === 'custom') {
      onSetAutoRefresh({...autoRefresh, status: AutoRefreshStatus.Disabled})
      return
    }

    if (autoRefresh.status === AutoRefreshStatus.Disabled) {
      if (autoRefresh.interval === 0) {
        onSetAutoRefresh({...autoRefresh, status: AutoRefreshStatus.Paused})
        return
      }

      onSetAutoRefresh({...autoRefresh, status: AutoRefreshStatus.Active})
    }
  }

  private get queryEditor(): JSX.Element {
    const {activeQuery} = this.props
    console.log(activeQuery)

    if (activeQuery.editMode === 'builder') {
      return <TimeMachineQueryBuilder />
    } else if (activeQuery.editMode === 'advanced') {
      return <TimeMachineFluxEditor />
    } else {
      return null
    }
  }
}

const mstp = (state: AppState) => {
  const timeRange = getTimeRange(state)
  const {autoRefresh} = getActiveTimeMachine(state)

  const activeQuery = getActiveQuery(state)

  return {
    timeRange,
    activeQuery,
    autoRefresh,
    isInCheckOverlay: getIsInCheckOverlay(state),
  }
}

const mdtp = {
  onSetTimeRange: setTimeRange,
  onSetAutoRefresh: setAutoRefresh,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueries)
