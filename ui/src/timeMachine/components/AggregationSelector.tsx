// Libraries
import React, {FunctionComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import AggregationContents from 'src/timeMachine/components/AggregationContents'

// Actions
import {
  selectBuilderFunction,
  selectAggregateWindow,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {
  getActiveQuery,
  getIsInCheckOverlay,
  getActiveWindowPeriod,
} from 'src/timeMachine/selectors'

// Types
import {AppState} from 'src/types'
import {FUNCTIONS} from '../constants/queryBuilder'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const AggregationSelector: FunctionComponent<Props> = ({
  selectedFunctions,
  isInCheckOverlay,
  isAutoFunction,
  isAutoWindowPeriod,
  isFillValues,
  aggregateWindow,
  onSelectFunction,
}) => {
  const functionList = isAutoFunction
    ? ['mean', 'median', 'first']
    : FUNCTIONS.map(f => f.name)

  return (
    <AggregationContents
      isAutoWindowPeriod={isAutoWindowPeriod}
      windowPeriod={aggregateWindow.period}
      isFillValues={isFillValues}
      isAutoFunction={isAutoFunction}
      functionList={functionList}
      selectedFunctions={selectedFunctions}
      onSelectFunction={onSelectFunction}
      isInCheckOverlay={isInCheckOverlay}
    />
  )
}

const mstp = (state: AppState) => {
  const {builderConfig} = getActiveQuery(state)
  const {functions, aggregateWindow} = builderConfig
  console.log(builderConfig)
  return {
    selectedFunctions: functions.map(f => f.name),
    aggregateWindow,
    autoWindowPeriod: getActiveWindowPeriod(state),
    isInCheckOverlay: getIsInCheckOverlay(state),
    isAutoFunction: true,
    isAutoWindowPeriod: false,
    isFillValues: true,
  }
}

const mdtp = {
  onSelectFunction: selectBuilderFunction,
  onSelectAggregateWindow: selectAggregateWindow,
}

const connector = connect(mstp, mdtp)

export default connector(AggregationSelector)
