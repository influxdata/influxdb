// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

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
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

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

const mstp = (state: AppState) => {
  const {draftQueries} = getActiveTimeMachine(state)
  const isInCheckOverlay = getIsInCheckOverlay(state)

  return {draftQueries, isInCheckOverlay}
}

const mdtp = {
  onAddQuery: addQuery,
}

const connector = connect(mstp, mdtp)

export default connector(QueryTabs)
