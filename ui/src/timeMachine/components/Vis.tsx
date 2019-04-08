// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import EmptyQueryView from 'src/shared/components/EmptyQueryView'
import VisSwitcher from 'src/timeMachine/components/VisSwitcher'

// Utils
import {getActiveTimeMachine, getTables} from 'src/timeMachine/selectors'

// Types
import {FluxTable, RemoteDataState, DashboardQuery, AppState} from 'src/types'

interface StateProps {
  queries: DashboardQuery[]
  tables: FluxTable[]
  loading: RemoteDataState
  errorMessage: string
  isInitialFetch: boolean
}

type Props = StateProps

const TimeMachineVis: SFC<Props> = ({
  queries,
  tables,
  loading,
  errorMessage,
  isInitialFetch,
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
        <VisSwitcher />
      </EmptyQueryView>
    </div>
  )
}

const mstp = (state: AppState) => {
  const {
    view,
    queryResults: {status: loading, errorMessage, isInitialFetch},
  } = getActiveTimeMachine(state)

  const {queries} = view.properties

  const tables = getTables(state)

  return {
    queries,
    tables,
    loading,
    errorMessage,
    isInitialFetch,
  }
}

export default connect<StateProps>(mstp)(TimeMachineVis)
