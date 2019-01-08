// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineFluxEditor from 'src/shared/components/TimeMachineFluxEditor'
import TimeMachineQueriesSwitcher from 'src/shared/components/TimeMachineQueriesSwitcher'
import TimeMachineQueryTab from 'src/shared/components/TimeMachineQueryTab'
import TimeMachineQueryBuilder from 'src/shared/components/TimeMachineQueryBuilder'
import TimeMachineInfluxQLEditor from 'src/shared/components/TimeMachineInfluxQLEditor'
import TimeMachineQueriesTimer from 'src/shared/components/TimeMachineQueriesTimer'
import SubmitQueryButton from 'src/shared/components/SubmitQueryButton'
import {
  Button,
  ComponentColor,
  ComponentSize,
  ButtonShape,
  IconFont,
} from 'src/clockface'

// Actions
import {addQuery} from 'src/shared/actions/v2/timeMachines'

// Utils
import {
  getActiveTimeMachine,
  getActiveQuery,
} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TimeMachineQueries.scss'

// Types
import {
  AppState,
  DashboardQuery,
  InfluxLanguage,
  QueryEditMode,
} from 'src/types/v2'
import {QueriesState} from 'src/shared/components/TimeSeries'

interface StateProps {
  activeQuery: DashboardQuery
  draftQueries: DashboardQuery[]
}

interface DispatchProps {
  onAddQuery: typeof addQuery
}

interface OwnProps {
  queriesState: QueriesState
}

type Props = StateProps & DispatchProps & OwnProps

const TimeMachineQueries: SFC<Props> = props => {
  const {activeQuery, queriesState, draftQueries, onAddQuery} = props

  let queryEditor

  if (activeQuery.editMode === QueryEditMode.Builder) {
    queryEditor = <TimeMachineQueryBuilder />
  } else if (activeQuery.type === InfluxLanguage.Flux) {
    queryEditor = <TimeMachineFluxEditor />
  } else if (activeQuery.type === InfluxLanguage.InfluxQL) {
    queryEditor = <TimeMachineInfluxQLEditor />
  }

  return (
    <div className="time-machine-queries">
      <div className="time-machine-queries--controls">
        <div className="time-machine-queries--tabs">
          {draftQueries.map((query, queryIndex) => (
            <TimeMachineQueryTab
              key={queryIndex}
              queryIndex={queryIndex}
              query={query}
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
          <TimeMachineQueriesTimer
            status={queriesState.loading}
            duration={queriesState.duration}
          />
          <TimeMachineQueriesSwitcher />
          {activeQuery.editMode !== QueryEditMode.Builder && (
            <SubmitQueryButton queryStatus={queriesState.loading} />
          )}
        </div>
      </div>
      <div className="time-machine-queries--body">{queryEditor}</div>
    </div>
  )
}

const mstp = (state: AppState) => {
  const {draftQueries, activeQueryIndex} = getActiveTimeMachine(state)
  const activeQuery = getActiveQuery(state)

  return {activeQuery, activeQueryIndex, draftQueries}
}

const mdtp = {
  onAddQuery: addQuery,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueries)
