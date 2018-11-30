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

import {
  Button,
  ComponentColor,
  ComponentSize,
  ComponentStatus,
  ButtonShape,
  IconFont,
} from 'src/clockface'

// Actions
import {submitScript, addQuery} from 'src/shared/actions/v2/timeMachines'

// Utils
import {getActiveTimeMachine} from 'src/shared/selectors/timeMachines'

// Styles
import 'src/shared/components/TimeMachineQueries.scss'

// Types
import {AppState} from 'src/types/v2'
import {RemoteDataState} from 'src/types'
import {TimeMachineEditor} from 'src/types/v2/timeMachine'

interface StateProps {
  activeQueryEditor: TimeMachineEditor
  queryCount: number
}

interface DispatchProps {
  onSubmitScript: typeof submitScript
  onAddQuery: typeof addQuery
}

interface OwnProps {
  queryStatus: RemoteDataState
}

type Props = StateProps & DispatchProps & OwnProps

const TimeMachineQueries: SFC<Props> = props => {
  const {
    activeQueryEditor,
    queryStatus,
    queryCount,
    onAddQuery,
    onSubmitScript,
  } = props

  const buttonStatus =
    queryStatus === RemoteDataState.Loading
      ? ComponentStatus.Loading
      : ComponentStatus.Default

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
            color={ComponentColor.Primary}
            onClick={onAddQuery}
          />
        </div>
        <div className="time-machine-queries--buttons">
          <TimeMachineQueriesSwitcher />
          <Button
            text="Submit"
            size={ComponentSize.Small}
            status={buttonStatus}
            onClick={onSubmitScript}
            color={ComponentColor.Primary}
          />
        </div>
      </div>
      <div className="time-machine-queries--body">
        {activeQueryEditor === TimeMachineEditor.QueryBuilder && (
          <TimeMachineQueryBuilder />
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
  const {activeQueryEditor, view} = getActiveTimeMachine(state)
  const queryCount: number = get(view, 'properties.queries.length', 0)

  return {activeQueryEditor, queryCount}
}

const mdtp = {
  onSubmitScript: submitScript,
  onAddQuery: addQuery,
}

export default connect<StateProps, DispatchProps, OwnProps>(
  mstp,
  mdtp
)(TimeMachineQueries)
