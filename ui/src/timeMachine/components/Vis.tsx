// Libraries
import React, {SFC} from 'react'
import {connect, ConnectedProps} from 'react-redux'
import {AutoSizer} from 'react-virtualized'
import classnames from 'classnames'

// Components
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import ViewLoadingSpinner from 'src/shared/components/ViewLoadingSpinner'
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
  getMosaicYColumnSelection,
  getMosaicFillColumnsSelection,
} from 'src/timeMachine/selectors'
import {getTimeRangeWithTimezone, getTimeZone} from 'src/dashboards/selectors'

// Types
import {RemoteDataState, AppState} from 'src/types'

// Selectors
import {getActiveTimeRange} from 'src/timeMachine/selectors/index'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

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
        <ViewLoadingSpinner loading={loading} />
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

const mstp = (state: AppState) => {
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
  const timeRange = getTimeRangeWithTimezone(state)
  const {
    alertBuilder: {type: checkType, thresholds: checkThresholds},
  } = state

  const giraffeResult = getVisTable(state)
  const xColumn = getXColumnSelection(state)

  let yColumn, fillColumns
  if (activeTimeMachine.view.properties.type === 'mosaic') {
    yColumn = getMosaicYColumnSelection(state)
    fillColumns = getMosaicFillColumnsSelection(state)
  } else {
    yColumn = getYColumnSelection(state)
    fillColumns = getFillColumnsSelection(state)
  }
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

const connector = connect(mstp)

export default connector(TimeMachineVis)
