// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {AutoSizer} from 'react-virtualized'

// Components
import EmptyQueryView from 'src/shared/components/EmptyQueryView'
import QueryViewSwitcher from 'src/shared/components/QueryViewSwitcher'
import RawFluxDataTable from 'src/timeMachine/components/RawFluxDataTable'

// Actions
import {setType} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine, getTables} from 'src/timeMachine/selectors'

// Types
import {FluxTable, RemoteDataState} from 'src/types'
import {View, NewView, TimeRange, DashboardQuery, AppState} from 'src/types/v2'
import {QueryViewProperties} from 'src/types/v2/dashboards'

interface StateProps {
  view: View | NewView
  timeRange: TimeRange
  queries: DashboardQuery[]
  isViewingRawData: boolean
  files: string[]
  tables: FluxTable[]
  loading: RemoteDataState
  errorMessage: string
  isInitialFetch: boolean
}

interface DispatchProps {
  onUpdateType: typeof setType
}

type Props = StateProps & DispatchProps

const TimeMachineVis: SFC<Props> = ({
  view,
  timeRange,
  queries,
  isViewingRawData,
  tables,
  loading,
  errorMessage,
  isInitialFetch,
  files,
}) => {
  return (
    <div className="time-machine--view">
      <EmptyQueryView
        errorMessage={errorMessage}
        tables={tables}
        loading={loading}
        isInitialFetch={isInitialFetch}
        queries={queries}
      >
        {isViewingRawData ? (
          <AutoSizer>
            {({width, height}) => (
              <RawFluxDataTable files={files} width={width} height={height} />
            )}
          </AutoSizer>
        ) : (
          <QueryViewSwitcher
            tables={tables}
            viewID="time-machine-view"
            loading={loading}
            timeRange={timeRange}
            properties={view.properties as QueryViewProperties}
          />
        )}
      </EmptyQueryView>
    </div>
  )
}

const mstp = (state: AppState) => {
  const {
    view,
    timeRange,
    isViewingRawData,
    queryResults: {status: loading, errorMessage, isInitialFetch, files},
  } = getActiveTimeMachine(state)

  const {queries} = view.properties

  const tables = getTables(state)

  return {
    view,
    timeRange,
    isViewingRawData,
    queries,
    tables,
    files,
    loading,
    errorMessage,
    isInitialFetch,
  }
}

const mdtp = {
  onUpdateType: setType,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineVis)
