// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'
import {AutoSizer} from 'react-virtualized'

// Components
import EmptyQueryView from 'src/shared/components/EmptyQueryView'
import QueryViewSwitcher from 'src/shared/components/QueryViewSwitcher'
import RawFluxDataTable from 'src/shared/components/RawFluxDataTable'

// Actions
import {setType} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Types
import {View, NewView, TimeRange, DashboardQuery, AppState} from 'src/types/v2'
import {QueryViewProperties} from 'src/types/v2/dashboards'
import {QueriesState} from 'src/shared/components/TimeSeries'

interface StateProps {
  view: View | NewView
  timeRange: TimeRange
  queries: DashboardQuery[]
  isViewingRawData: boolean
}

interface DispatchProps {
  onUpdateType: typeof setType
}

interface OwnProps {
  queriesState: QueriesState
}

type Props = StateProps & DispatchProps & OwnProps

const TimeMachineVis: SFC<Props> = props => {
  const {view, timeRange, queries, isViewingRawData} = props
  const {tables, loading, error, isInitialFetch, files} = props.queriesState

  return (
    <div className="time-machine-top">
      <div className="time-machine-vis">
        <div className="graph-container">
          <EmptyQueryView
            error={error}
            tables={tables}
            loading={loading}
            isInitialFetch={isInitialFetch}
            queries={queries}
          >
            {isViewingRawData ? (
              <AutoSizer>
                {({width, height}) => (
                  <RawFluxDataTable
                    files={files}
                    width={width}
                    height={height}
                  />
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
      </div>
    </div>
  )
}

const mstp = (state: AppState) => {
  const timeMachine = getActiveTimeMachine(state)
  const queries = get(timeMachine, 'view.properties.queries', [])

  return {
    view: timeMachine.view,
    timeRange: timeMachine.timeRange,
    isViewingRawData: timeMachine.isViewingRawData,
    queries,
  }
}

const mdtp = {
  onUpdateType: setType,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineVis)
