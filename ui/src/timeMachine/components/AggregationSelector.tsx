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
  setIsAutoWindowPeriod,
  setIsAutoFunction,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {
  getActiveQuery,
  getIsInCheckOverlay,
  getActiveWindowPeriod,
  getActiveTimeMachine,
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
  aggregateWindow: {period, fillValues},
  onSelectFunction,
  onSetAggregateFillValues,
  onSetIsAutoFunction,
  onSetIsAutoWindowPeriod,
}) => {
  const functionList = isAutoFunction
    ? ['mean', 'median', 'first']
    : FUNCTIONS.map(f => f.name)

  const onChangeFillValues = () => {
    onSetAggregateFillValues(!fillValues)
  }

  return (
    <AggregationContents
      isAutoWindowPeriod={isAutoWindowPeriod}
      onSetIsAutoWindowPeriod={onSetIsAutoWindowPeriod}
      onSetIsAutoFunction={onSetIsAutoFunction}
      windowPeriod={period} //BE done
      isFillValues={fillValues} //BE
      isAutoFunction={isAutoFunction}
      functionList={functionList} //BE done
      selectedFunctions={selectedFunctions}
      onSelectFunction={onSelectFunction}
      isInCheckOverlay={isInCheckOverlay}
      onChangeFillValues={onChangeFillValues}
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
  onSetIsAutoWindowPeriod: setIsAutoWindowPeriod,
  onSetIsAutoFunction: setIsAutoFunction,
}

const connector = connect(mstp, mdtp)

export default connector(AggregationSelector)
