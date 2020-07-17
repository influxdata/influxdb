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
  getActiveWindowPeriod,
  getActiveTimeMachine,
} from 'src/timeMachine/selectors'

// Constants
import {FUNCTIONS, AUTO_FUNCTIONS} from '../constants/queryBuilder'

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

  return (
    <AggregationContents
      isAutoWindowPeriod={isAutoWindowPeriod}
      onSetWindowPeriodSelectionMode={onSetWindowPeriodSelectionMode}
      onSetFunctionSelectionMode={onSetFunctionSelectionMode}
      windowPeriod={period} //BE done
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
    selectedFunctions: functions.map(f => f.name),
    aggregateWindow,
    autoWindowPeriod: getActiveWindowPeriod(state),
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
