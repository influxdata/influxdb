// Libraries
import React, {FunctionComponent} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import AggregationContents from 'src/timeMachine/components/AggregationContents'

// Actions
import {
  selectBuilderFunction,
  selectAggregateWindow,
  setAggregateFillValues,
  setWindowPeriodSelectionMode,
  setFunctionSelectionMode,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {
  getActiveQuery,
  getIsInCheckOverlay,
  getActiveTimeMachine,
  getWindowPeriodFromTimeRange,
} from 'src/timeMachine/selectors'

// Constants
import {
  FUNCTIONS,
  AUTO_FUNCTIONS,
  AGG_WINDOW_AUTO,
} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const AggregationSelector: FunctionComponent<Props> = ({
  selectedFunctions,
  isInCheckOverlay,
  isAutoFunction,
  isAutoWindowPeriod,
  aggregateWindow: {period, fillValues},
  autoWindowPeriod,
  onSelectFunction,
  onSetAggregateFillValues,
  onSetFunctionSelectionMode,
  onSetWindowPeriodSelectionMode,
  onSelectAggregateWindow,
}) => {
  const functionList = isAutoFunction
    ? AUTO_FUNCTIONS.map(f => f.name)
    : FUNCTIONS.map(f => f.name)

  const onChangeFillValues = () => {
    onSetAggregateFillValues(!fillValues)
  }

  let durationDisplay = period

  if (!period || period === AGG_WINDOW_AUTO) {
    durationDisplay = autoWindowPeriod
      ? `${AGG_WINDOW_AUTO} (${autoWindowPeriod})`
      : AGG_WINDOW_AUTO
  }

  return (
    <AggregationContents
      isAutoWindowPeriod={isAutoWindowPeriod}
      onSetWindowPeriodSelectionMode={onSetWindowPeriodSelectionMode}
      onSetFunctionSelectionMode={onSetFunctionSelectionMode}
      durationDisplay={durationDisplay} //BE done
      isFillValues={fillValues} //BE
      isAutoFunction={isAutoFunction}
      functionList={functionList} //BE done
      selectedFunctions={selectedFunctions}
      onSelectFunction={onSelectFunction}
      isInCheckOverlay={isInCheckOverlay}
      onChangeFillValues={onChangeFillValues}
      onSelectAggregateWindow={onSelectAggregateWindow}
    />
  )
}

const mstp = (state: AppState) => {
  const {
    queryBuilder: {isAutoFunction, isAutoWindowPeriod},
  } = getActiveTimeMachine(state)
  const {builderConfig} = getActiveQuery(state)
  const {functions, aggregateWindow} = builderConfig
  return {
    autoWindowPeriod: getWindowPeriodFromTimeRange(state),
    selectedFunctions: functions.map(f => f.name),
    aggregateWindow,
    isInCheckOverlay: getIsInCheckOverlay(state),
    isAutoFunction: isAutoFunction,
    isAutoWindowPeriod: isAutoWindowPeriod,
  }
}

const mdtp = {
  onSelectFunction: selectBuilderFunction,
  onSelectAggregateWindow: selectAggregateWindow,
  onSetAggregateFillValues: setAggregateFillValues,
  onSetWindowPeriodSelectionMode: setWindowPeriodSelectionMode,
  onSetFunctionSelectionMode: setFunctionSelectionMode,
}

const connector = connect(mstp, mdtp)

export default connector(AggregationSelector)
