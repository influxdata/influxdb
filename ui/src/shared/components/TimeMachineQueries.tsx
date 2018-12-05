// Libraries
import React, {SFC} from 'react'
import {connect} from 'react-redux'
import {range, get} from 'lodash'

// Components
import TimeMachineFluxEditor from 'src/shared/components/TimeMachineFluxEditor'
import TimeMachineQueriesSwitcher from 'src/shared/components/TimeMachineQueriesSwitcher'
import TimeMachineQueryTab from 'src/shared/components/TimeMachineQueryTab'
import TimeMachineQueryBuilder from 'src/shared/components/TimeMachineQueryBuilder'
import TimeMachineInfluxQLEditor from 'src/shared/components/TimeMachineInfluxQLEditor'
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
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TimeMachineQueries.scss'

// Types
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'
import {TimeMachineEditor} from 'src/types/v2/timeMachine'

interface StateProps {
  activeQueryIndex: number
  activeQueryEditor: TimeMachineEditor
  queryCount: number
}

interface DispatchProps {
  onAddQuery: typeof addQuery
}

interface OwnProps {
  queryStatus: RemoteDataState
}

type Props = StateProps & DispatchProps & OwnProps

const TimeMachineQueries: SFC<Props> = props => {
  const {
    activeQueryEditor,
    activeQueryIndex,
    queryStatus,
    queryCount,
    onAddQuery,
  } = props

  return (
    <div className="time-machine-queries">
      <div className="time-machine-queries--controls">
        <div className="time-machine-queries--tabs">
          {range(queryCount).map(i => (
            <TimeMachineQueryTab key={i} queryIndex={i} />
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
          <TimeMachineQueriesSwitcher />
          <SubmitQueryButton queryStatus={queryStatus} />
        </div>
      </div>
      <div className="time-machine-queries--body">
        {activeQueryEditor === TimeMachineEditor.QueryBuilder && (
          <TimeMachineQueryBuilder key={activeQueryIndex} />
        )}
        {activeQueryEditor === TimeMachineEditor.FluxEditor && (
          <TimeMachineFluxEditor />
        )}
        {activeQueryEditor === TimeMachineEditor.InfluxQLEditor && (
          <TimeMachineInfluxQLEditor />
        )}
      </div>
    </div>
  )
}

const mstp = (state: AppState) => {
  const {activeQueryEditor, view, activeQueryIndex} = getActiveTimeMachine(
    state
  )

  const queryCount: number = get(view, 'properties.queries.length', 0)

  return {activeQueryEditor, activeQueryIndex, queryCount}
}

const mdtp = {
  onAddQuery: addQuery,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueries)
