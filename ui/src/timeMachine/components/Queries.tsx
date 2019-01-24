// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineFluxEditor from 'src/timeMachine/components/TimeMachineFluxEditor'
import CSVExportButton from 'src/shared/components/CSVExportButton'
import TimeMachineQueriesSwitcher from 'src/timeMachine/components/QueriesSwitcher'
import TimeMachineRefreshDropdown from 'src/timeMachine/components/RefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import TimeMachineQueryTab from 'src/timeMachine/components/QueryTab'
import TimeMachineQueryBuilder from 'src/timeMachine/components/QueryBuilder'
import TimeMachineInfluxQLEditor from 'src/timeMachine/components/TimeMachineInfluxQLEditor'
import SubmitQueryButton from 'src/timeMachine/components/SubmitQueryButton'
import {
  Button,
  ComponentSpacer,
  Alignment,
  ComponentColor,
  ComponentSize,
  ButtonShape,
  IconFont,
  SlideToggle,
} from 'src/clockface'

// Actions
import {addQuery} from 'src/timeMachine/actions'
import {setTimeRange, setIsViewingRawData} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'

// Styles
import 'src/timeMachine/components/Queries.scss'

// Types
import {
  AppState,
  DashboardQuery,
  InfluxLanguage,
  QueryEditMode,
  TimeRange,
} from 'src/types/v2'
import {DashboardDraftQuery} from 'src/types/v2/dashboards'
import {QueriesState} from 'src/shared/components/TimeSeries'

interface StateProps {
  activeQuery: DashboardQuery
  draftQueries: DashboardDraftQuery[]
  timeRange: TimeRange
  isViewingRawData: boolean
}

interface DispatchProps {
  onAddQuery: typeof addQuery
  onSetTimeRange: typeof setTimeRange
  onSetIsViewingRawData: typeof setIsViewingRawData
}

interface OwnProps {
  queriesState: QueriesState
}

type Props = StateProps & DispatchProps & OwnProps

class TimeMachineQueries extends PureComponent<Props> {
  public render() {
    const {
      queriesState,
      draftQueries,
      onAddQuery,
      timeRange,
      onSetTimeRange,
      isViewingRawData,
      queriesState: {files},
    } = this.props

    return (
      <div className="time-machine-queries">
        <div className="time-machine-queries--controls">
          <div className="time-machine-queries--tabs">
            {draftQueries.map((query, queryIndex) => (
              <TimeMachineQueryTab
                key={queryIndex}
                queryIndex={queryIndex}
                query={query}
                queriesState={queriesState}
              />
            ))}
            <Button
              customClass="time-machine-queries--new"
              shape={ButtonShape.Square}
              icon={IconFont.PlusSkinny}
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              onClick={onAddQuery}
            />
          </div>
          <div className="time-machine-queries--buttons">
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
              <TimeMachineQueriesSwitcher />
              <SubmitQueryButton queryStatus={queriesState.loading} />
            </ComponentSpacer>
          </div>
        </div>
        <div className="time-machine-queries--body">{this.queryEditor}</div>
      </div>
    )
  }

  private get queryEditor(): JSX.Element {
    const {activeQuery} = this.props

    if (activeQuery.editMode === QueryEditMode.Builder) {
      return <TimeMachineQueryBuilder />
    } else if (activeQuery.type === InfluxLanguage.Flux) {
      return <TimeMachineFluxEditor />
    } else if (activeQuery.type === InfluxLanguage.InfluxQL) {
      return <TimeMachineInfluxQLEditor />
    }
  }

  private handleToggleIsViewingRawData = () => {
    const {isViewingRawData, onSetIsViewingRawData} = this.props

    onSetIsViewingRawData(!isViewingRawData)
  }
}

const mstp = (state: AppState) => {
  const {draftQueries, timeRange, isViewingRawData} = getActiveTimeMachine(
    state
  )

  const activeQuery = getActiveQuery(state)

  return {timeRange, activeQuery, draftQueries, isViewingRawData}
}

const mdtp = {
  onAddQuery: addQuery,
  onSetTimeRange: setTimeRange,
  onSetIsViewingRawData: setIsViewingRawData,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueries)
