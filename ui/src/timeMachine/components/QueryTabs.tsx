// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineQueryTab from 'src/timeMachine/components/QueryTab'
import {
  SquareButton,
  IconFont,
  ComponentSize,
  ComponentColor,
} from '@influxdata/clockface'

// Actions
import {addQuery} from 'src/timeMachine/actions'

// Utils
import {
  getActiveTimeMachine,
  getIsInCheckOverlay,
} from 'src/timeMachine/selectors'

// Types
import {AppState, DashboardDraftQuery} from 'src/types'

interface StateProps {
  draftQueries: DashboardDraftQuery[]
  isInCheckOverlay: boolean
}

interface DispatchProps {
  onAddQuery: () => any
}

type Props = StateProps & DispatchProps

const QueryTabs: FC<Props> = ({draftQueries, isInCheckOverlay, onAddQuery}) => {
  return (
    <div className="time-machine-queries--tabs">
      {draftQueries.map((query, queryIndex) => (
        <TimeMachineQueryTab
          key={queryIndex}
          queryIndex={queryIndex}
          query={query}
        />
      ))}
      {!isInCheckOverlay && (
        <SquareButton
          className="time-machine-queries--new"
          icon={IconFont.PlusSkinny}
          size={ComponentSize.Small}
          color={ComponentColor.Default}
          onClick={onAddQuery}
        />
      )}
    </div>
  )
}

const mstp = (state: AppState): StateProps => {
  const {draftQueries} = getActiveTimeMachine(state)
  const isInCheckOverlay = getIsInCheckOverlay(state)

  return {draftQueries, isInCheckOverlay}
}

const mdtp = {
  onAddQuery: addQuery,
}

export default connect<StateProps, DispatchProps>(mstp, mdtp)(QueryTabs)
