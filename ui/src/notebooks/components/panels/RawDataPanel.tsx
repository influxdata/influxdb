// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {FromFluxResult} from '@influxdata/giraffe'
import {AutoSizer} from 'react-virtualized'

// Components
import EmptyQueryView, {ErrorFormat} from 'src/shared/components/EmptyQueryView'
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'
import ErrorBoundary from 'src/shared/components/ErrorBoundary'
import NotebookPanel from 'src/notebooks/components/NotebookPanel'
import CSVExportButton from 'src/shared/components/CSVExportButton'

// Utils
import {getActiveTimeMachine} from 'src/timeMachine/selectors'
import {checkResultsLength} from 'src/shared/utils/vis'
import {getVisTable} from 'src/timeMachine/selectors'
import {getTimeRange} from 'src/dashboards/selectors'

// Types
import {
  RemoteDataState,
  AppState,
  QueryViewProperties,
  TimeRange,
  StatusRow,
} from 'src/types'

// Selectors
import {getActiveTimeRange} from 'src/timeMachine/selectors/index'

interface OwnProps {
  id: string
  onChangeID: (id: string) => void
  dataSourceName?: string
}

interface StateProps {
  timeRange: TimeRange | null
  loading: RemoteDataState
  errorMessage: string
  files: string[]
  viewProperties: QueryViewProperties
  isInitialFetch: boolean
  giraffeResult: FromFluxResult
  statuses: StatusRow[][]
}

type Props = OwnProps & StateProps

const TimeMachineVis: SFC<Props> = ({
  id,
  onChangeID,
  dataSourceName,
  loading,
  errorMessage,
  isInitialFetch,
  files,
  viewProperties,
  giraffeResult,
}) => {
  return (
    <NotebookPanel
      id={id}
      title={id}
      dataSourceName={dataSourceName}
      onTitleChange={onChangeID}
      controlsRight={<CSVExportButton />}
    >
      <div className="notebook-panel--results">
      <ErrorBoundary>
        <EmptyQueryView
          loading={loading}
          errorFormat={ErrorFormat.Scroll}
          errorMessage={errorMessage}
          isInitialFetch={isInitialFetch}
          queries={viewProperties.queries}
          hasResults={checkResultsLength(giraffeResult)}
          >
          <AutoSizer>
            {({width, height}) =>
              width &&
              height && (
                <RawFluxDataTable files={files} width={width} height={height} />
                )
              }
          </AutoSizer>
        </EmptyQueryView>
      </ErrorBoundary>
              </div>
    </NotebookPanel>
  )
}

const mstp = (state: AppState): StateProps => {
  const activeTimeMachine = getActiveTimeMachine(state)
  const {
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

  const giraffeResult = getVisTable(state)

  return {
    loading,
    errorMessage,
    isInitialFetch,
    files,
    viewProperties,
    giraffeResult,
    timeRange: getActiveTimeRange(timeRange, viewProperties.queries),
    statuses,
  }
}

export default connect<StateProps>(mstp)(TimeMachineVis)
