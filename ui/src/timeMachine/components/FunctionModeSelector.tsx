// Libraries
import React, {FunctionComponent, useState} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import {
  SelectGroup,
  ButtonShape,
  FlexBox,
  FlexDirection,
  ComponentSize,
  AlignItems,
} from '@influxdata/clockface'
import BuilderCard from 'src/timeMachine/components//builderCard/BuilderCard'
import SelectorList from 'src/timeMachine/components/SelectorList'

//Actions
import {
  multiSelectBuilderFunction,
  singleSelectBuilderFunction,
  setFunctions,
} from 'src/timeMachine/actions/queryBuilder'

// Utils
import {getActiveQuery} from 'src/timeMachine/selectors'

// Constants
import {AUTO_FUNCTIONS, FUNCTIONS} from 'src/timeMachine/constants/queryBuilder'

// Types
import {AppState} from 'src/types'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

const FunctionSelector: FunctionComponent<Props> = ({
  onSetFunctions,
  selectedFunctions,
  onSingleSelectBuilderFunction,
  onMultiSelectBuilderFunction,
}) => {
  const autoFunctions = AUTO_FUNCTIONS.map(f => f.name)

  const [isAutoFunction, setIsAutoFunction] = useState(
    selectedFunctions.length === 1 &&
      autoFunctions.includes(selectedFunctions[0])
  )

  const functionList = isAutoFunction
    ? autoFunctions
    : FUNCTIONS.map(f => f.name)

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

  const onSelectFunction = isAutoFunction
    ? onSingleSelectBuilderFunction
    : onMultiSelectBuilderFunction

  return (
    <>
      <BuilderCard.Header
        title="Aggregate Function"
        className="aggregation-selector-header"
      />
      <BuilderCard.Menu className="aggregation-selector-menu">
        <FlexBox
          direction={FlexDirection.Column}
          margin={ComponentSize.ExtraSmall}
          alignItems={AlignItems.Stretch}
        >
          <SelectGroup
            shape={ButtonShape.StretchToFit}
            size={ComponentSize.ExtraSmall}
          >
            <SelectGroup.Option
              name="custom"
              id="custom-function"
              testID="custom-function"
              active={!isAutoFunction}
              value="custom"
              onClick={setFunctionSelectionMode}
              titleText="Custom"
            >
              Custom
            </SelectGroup.Option>
            <SelectGroup.Option
              name="auto"
              id="auto-function"
              testID="auto-function"
              active={isAutoFunction}
              value="auto"
              onClick={setFunctionSelectionMode}
              titleText="Auto"
            >
              Auto
            </SelectGroup.Option>
          </SelectGroup>
        </FlexBox>
      </BuilderCard.Menu>
      <SelectorList
        items={functionList}
        selectedItems={selectedFunctions}
        onSelectItem={onSelectFunction}
        multiSelect={!isAutoFunction}
      />
    </>
  )
}

const mstp = (state: AppState) => {
  const {builderConfig} = getActiveQuery(state)
  const {functions} = builderConfig
  return {
    selectedFunctions: functions.map(f => f.name),
  }
}

const mdtp = {
  onMultiSelectBuilderFunction: multiSelectBuilderFunction,
  onSingleSelectBuilderFunction: singleSelectBuilderFunction,
  onSetFunctions: setFunctions,
}

const connector = connect(mstp, mdtp)

export default connector(FunctionSelector)
