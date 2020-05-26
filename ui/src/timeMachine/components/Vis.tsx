// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {FromFluxResult} from '@influxdata/giraffe'
import {AutoSizer} from 'react-virtualized'
import classnames from 'classnames'

// Components
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import ErrorBoundary from 'src/shared/components/ErrorBoundary'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {checkResultsLength} from 'src/shared/utils/vis'
import {
  getVisTable,
  getXColumnSelection,
  getYColumnSelection,
  getFillColumnsSelection,
  getSymbolColumnsSelection,
} from 'src/timeMachine/selectors'
import {getTimeRange, getTimeZone} from 'src/dashboards/selectors'

// Types
import {
  RemoteDataState,
  AppState,
  QueryViewProperties,
  TimeZone,
  TimeRange,
  StatusRow,
  CheckType,
  Threshold,
} from 'src/types'

// Selectors
import {getActiveTimeRange} from 'src/timeMachine/selectors/index'

interface StateProps {
  timeRange: TimeRange | null
  loading: RemoteDataState
  errorMessage: string
  files: string[]
  viewProperties: QueryViewProperties
  isInitialFetch: boolean
  isViewingRawData: boolean
  giraffeResult: FromFluxResult
  xColumn: string
  yColumn: string
  checkType: CheckType
  checkThresholds: Threshold[]
  fillColumns: string[]
  symbolColumns: string[]
  timeZone: TimeZone
  statuses: StatusRow[][]
}

type Props = StateProps

const TimeMachineVis: SFC<Props> = ({
  loading,
  errorMessage,
  timeRange,
  isInitialFetch,
  isViewingRawData,
  files,
  checkType,
  checkThresholds,
  viewProperties,
  giraffeResult,
  xColumn,
  yColumn,
  fillColumns,
  symbolColumns,
  timeZone,
  statuses,
}) => {
  // If the current selections for `xColumn`/`yColumn`/ etc. are invalid given
  // the current Flux response, attempt to make a valid selection instead. This
  // fallback logic is contained within the selectors that supply each of these
  // props. Note that in a dashboard context, we display an error instead of
  // attempting to fall back to an valid selection.
  const resolvedViewProperties = {
    ...viewProperties,
    xColumn,
    yColumn,
    fillColumns,
    symbolColumns,
  }

  const noQueries =
    loading === RemoteDataState.NotStarted || !viewProperties.queries.length
  const timeMachineViewClassName = classnames('time-machine--view', {
    'time-machine--view__empty': noQueries,
  })

  return (
    <div className={timeMachineViewClassName}>
      <ErrorBoundary>
        <EmptyQueryView
          loading={loading}
          errorFormat={ErrorFormat.Scroll}
          errorMessage={errorMessage}
          isInitialFetch={isInitialFetch}
          queries={viewProperties.queries}
          hasResults={checkResultsLength(giraffeResult)}
        >
          {isViewingRawData ? (
            <AutoSizer>
              {({width, height}) =>
                width &&
                height && (
                  <RawFluxDataTable
                    files={files}
                    width={width}
                    height={height}
                  />
                )
              }
            </AutoSizer>
          ) : (
            <ViewSwitcher
              giraffeResult={giraffeResult}
              timeRange={timeRange}
              files={files}
              loading={loading}
              properties={resolvedViewProperties}
              checkType={checkType}
              checkThresholds={checkThresholds}
              timeZone={timeZone}
              statuses={statuses}
              theme="dark"
            />
          )}
        </EmptyQueryView>
      </ErrorBoundary>
    </div>
  )
}

const mstp = (state: AppState): StateProps => {
  const activeTimeMachine = getActiveTimeMachine(state)
  const {
    isViewingRawData,
    view: {properties: viewProperties},
    queryResults: {
      status: loading,
      errorMessage,
      isInitialFetch,
      files,
      statuses,
    },
  } = activeTimeMachine
  const timeRange = getTimeRange(state)
  const {
    alertBuilder: {type: checkType, thresholds: checkThresholds},
  } = state

  const giraffeResult = getVisTable(state)
  const xColumn = getXColumnSelection(state)
  const yColumn = getYColumnSelection(state)
  const fillColumns = getFillColumnsSelection(state)
  const symbolColumns = getSymbolColumnsSelection(state)

  const timeZone = getTimeZone(state)

  return {
    loading,
    checkType,
    checkThresholds,
    errorMessage,
    isInitialFetch,
    files,
    viewProperties,
    isViewingRawData,
    giraffeResult,
    xColumn,
    yColumn,
    fillColumns,
    symbolColumns,
    timeZone,
    timeRange: getActiveTimeRange(timeRange, viewProperties.queries),
    statuses,
  }
}

export default connect<StateProps>(mstp)(TimeMachineVis)
