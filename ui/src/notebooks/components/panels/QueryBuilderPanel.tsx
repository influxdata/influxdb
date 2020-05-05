// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineFluxEditor from 'src/timeMachine/components/TimeMachineFluxEditor'
import TimeMachineQueriesSwitcher from 'src/timeMachine/components/QueriesSwitcher'
import TimeMachineRefreshDropdown from 'src/timeMachine/components/RefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import TimeMachineQueryBuilder from 'src/timeMachine/components/QueryBuilder'
import SubmitQueryButton from 'src/timeMachine/components/SubmitQueryButton'
import EditorShortcutsToolTip from 'src/timeMachine/components/EditorShortcutsTooltip'
import NotebookPanel from 'src/notebooks/components/NotebookPanel'

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

    let controlsRight = (
      <>
        {activeQuery.editMode === 'advanced' && <EditorShortcutsToolTip />}
        <SubmitQueryButton />
      </>
    )

    if (!isInCheckOverlay) {
      controlsRight = (
        <>
          {activeQuery.editMode === 'advanced' && <EditorShortcutsToolTip />}
          <TimeMachineRefreshDropdown />
          <TimeRangeDropdown
            timeRange={timeRange}
            onSetTimeRange={this.handleSetTimeRange}
          />
          <SubmitQueryButton />
        </>
      )
    }

    return (
      <NotebookPanel
        title="Query Builder"
        controlsLeft={<TimeMachineQueriesSwitcher />}
        controlsRight={controlsRight}
      >
        {this.queryEditor}
      </NotebookPanel>
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
