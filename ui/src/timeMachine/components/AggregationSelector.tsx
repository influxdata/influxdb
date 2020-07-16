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
  aggregateWindow: {period, fillValues},
  onSelectFunction,
  onSetAggregateFillValues,
}) => {
  const functionList = isAutoFunction
    ? FUNCTIONS.map(f => f.name)
    : ['mean', 'median', 'first']

  const onChangeFillValues = () => {
    onSetAggregateFillValues(!fillValues)
  }

  return (
    <AggregationContents
      isAutoWindowPeriod={isAutoWindowPeriod}
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
  const {builderConfig} = getActiveQuery(state)
  const {functions, aggregateWindow} = builderConfig
  return {
    selectedFunctions: functions.map(f => f.name),
    aggregateWindow,
    autoWindowPeriod: getActiveWindowPeriod(state),
    isInCheckOverlay: getIsInCheckOverlay(state),
    isAutoFunction: true,
    isAutoWindowPeriod: false,
  }
}

const mdtp = {
  onSelectFunction: selectBuilderFunction,
  onSelectAggregateWindow: selectAggregateWindow,
  onSetAggregateFillValues: setAggregateFillValues,
}

const connector = connect(mstp, mdtp)

export default connector(AggregationSelector)
