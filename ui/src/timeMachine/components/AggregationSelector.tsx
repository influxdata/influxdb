// Libraries
import React, {FunctionComponent, useState} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import AggregationContents from 'src/timeMachine/components/AggregationContents'

// Actions
import {
  multiSelectBuilderFunction,
  singleSelectBuilderFunction,
  selectAggregateWindow,
  setAggregateFillValues,
  setWindowPeriodSelectionMode,
  setFunctions,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {
  getActiveQuery,
  getIsInCheckOverlay,
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
  aggregateWindow: {period, fillValues},
  autoWindowPeriod,
  onMultiSelectBuilderFunction,
  onSingleSelectBuilderFunction,
  onSetAggregateFillValues,
  onSetWindowPeriodSelectionMode,
  onSelectAggregateWindow,
  onSetFunctions,
}) => {
  const autoFunctions = AUTO_FUNCTIONS.map(f => f.name)

  const [isAutoFunction, setIsAutoFunction] = useState(
    selectedFunctions.length === 1 &&
      autoFunctions.includes(selectedFunctions[0])
  )

  const functionList = isAutoFunction
    ? autoFunctions
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

  const setFunctionSelectionMode = (mode: 'custom' | 'auto') => {
    if (mode === 'custom') {
      setIsAutoFunction(false)
      return
    }
    const newFunctions = selectedFunctions.filter(f =>
      autoFunctions.includes(f)
    )
    if (newFunctions.length === 0) {
      onSetFunctions([autoFunctions[0]])
    } else if (newFunctions.length > 1) {
      onSetFunctions([newFunctions[0]])
    } else {
      onSetFunctions(newFunctions)
    }

    setIsAutoFunction(true)
  }

  return (
    <AggregationContents
      isAutoWindowPeriod={period === AGG_WINDOW_AUTO}
      onSetWindowPeriodSelectionMode={onSetWindowPeriodSelectionMode}
      durationDisplay={durationDisplay}
      isFillValues={fillValues}
      functionList={functionList}
      isAutoFunction={isAutoFunction}
      onSetFunctionSelectionMode={setFunctionSelectionMode}
      selectedFunctions={selectedFunctions}
      onSelectFunction={
        isAutoFunction
          ? onSingleSelectBuilderFunction
          : onMultiSelectBuilderFunction
      }
      isInCheckOverlay={isInCheckOverlay}
      onChangeFillValues={onChangeFillValues}
      onSelectAggregateWindow={onSelectAggregateWindow}
    />
  )
}

const mstp = (state: AppState) => {
  const {builderConfig} = getActiveQuery(state)
  const {functions, aggregateWindow} = builderConfig
  return {
    autoWindowPeriod: getWindowPeriodFromTimeRange(state),
    selectedFunctions: functions.map(f => f.name),
    aggregateWindow,
    isInCheckOverlay: getIsInCheckOverlay(state),
  }
}

const mdtp = {
  onMultiSelectBuilderFunction: multiSelectBuilderFunction,
  onSingleSelectBuilderFunction: singleSelectBuilderFunction,
  onSelectAggregateWindow: selectAggregateWindow,
  onSetAggregateFillValues: setAggregateFillValues,
  onSetWindowPeriodSelectionMode: setWindowPeriodSelectionMode,
  onSetFunctions: setFunctions,
}

const connector = connect(mstp, mdtp)

export default connector(AggregationSelector)
