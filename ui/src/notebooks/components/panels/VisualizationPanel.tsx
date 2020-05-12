// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {FromFluxResult} from '@influxdata/giraffe'

// Components
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import ViewSwitcher from 'src/shared/components/ViewSwitcher'
import ErrorBoundary from 'src/shared/components/ErrorBoundary'
import NotebookPanel from 'src/notebooks/components/NotebookPanel'
import ViewOptions from 'src/timeMachine/components/view_options/ViewOptions'
import VisOptionsButton from 'src/timeMachine/components/VisOptionsButton'
import ViewTypeDropdown from 'src/timeMachine/components/view_options/ViewTypeDropdown'

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
import {getTimeRange} from 'src/dashboards/selectors'

// Actions
import {setIsViewingRawData} from 'src/timeMachine/actions'

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
  isViewingVisOptions: boolean
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

interface DispatchProps {
  onSetIsViewingRawData: typeof setIsViewingRawData
}

interface OwnProps {
  dataSourceName: string
  title: string
  onChangeTitle: (id: string) => void
}

type Props = OwnProps & StateProps & DispatchProps

const VisualizationPanel: SFC<Props> = ({
  loading,
  errorMessage,
  timeRange,
  isInitialFetch,
  isViewingVisOptions,
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
  onSetIsViewingRawData,
  dataSourceName,
  title,
  onChangeTitle,
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

  const handleRemovePanel = (): void => {
    onSetIsViewingRawData(false)
  }

  const controls = (
    <>
      <ViewTypeDropdown />
      <VisOptionsButton />
    </>
  )

  return (
    <NotebookPanel
      id={title}
      title={title}
      onTitleChange={onChangeTitle}
      dataSourceName={dataSourceName}
      onRemove={handleRemovePanel}
      controlsRight={controls}
    >
      <ErrorBoundary>
        <EmptyQueryView
          loading={loading}
          errorFormat={ErrorFormat.Scroll}
          errorMessage={errorMessage}
          isInitialFetch={isInitialFetch}
          queries={viewProperties.queries}
          hasResults={checkResultsLength(giraffeResult)}
        >
          <div className="notebook-panel--visualization">
            <div className="notebook-panel--view">
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
            </div>
            {isViewingVisOptions && <ViewOptions />}
          </div>
        </EmptyQueryView>
      </ErrorBoundary>
    </NotebookPanel>
  )
}

const mstp = (state: AppState): StateProps => {
  const activeTimeMachine = getActiveTimeMachine(state)
  const {
    isViewingVisOptions,
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

  const timeZone = state.app.persisted.timeZone

  return {
    loading,
    checkType,
    checkThresholds,
    errorMessage,
    isInitialFetch,
    files,
    viewProperties,
    isViewingVisOptions,
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

const mdtp = {
  onSetIsViewingRawData: setIsViewingRawData,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(VisualizationPanel)
