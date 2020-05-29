// Libraries
import React, {useMemo, useEffect, FunctionComponent} from 'react'
import {cloneDeep} from 'lodash'

// Components
import ThresholdSetting from 'src/shared/components/ThresholdSetting'
import {
  Button,
  ButtonShape,
  IconFont,
  FlexBox,
  ComponentSize,
  FlexDirection,
  AlignItems,
} from '@influxdata/clockface'

// Utils
import {useOneWayReducer} from 'src/shared/utils/useOneWayReducer'
import {
  sortThresholds,
  validateThresholds,
  addThreshold,
} from 'src/shared/utils/thresholds'

// Types
import {Color} from 'src/types'

interface Props {
  thresholds: Color[]
  onSetThresholds: (thresholds: Color[]) => void
}

interface State {
  thresholds: Color[]
  inputs: {[thresholdID: string]: string}
  errors: {[thresholdID: string]: string}
  isValid: boolean
  isDirty: boolean
}

type Action =
  | {type: 'COLOR_CHANGED'; id: string; name: string; hex: string}
  | {type: 'VALUE_CHANGED'; id: string; value: string}
  | {type: 'VALUE_BLURRED'; id: string}
  | {type: 'THRESHOLD_REMOVED'; id: string}
  | {type: 'THRESHOLD_ADDED'}

const reducer = (state: State, action: Action): State => {
  switch (action.type) {
    case 'COLOR_CHANGED': {
      const {id, name, hex} = action

      const thresholds = state.thresholds.map(threshold =>
        threshold.id === id ? {...threshold, name, hex} : threshold
      )

      return {...state, thresholds, isDirty: true}
    }

    case 'VALUE_CHANGED': {
      const {id, value} = action

      const inputs = {...state.inputs, [id]: value}

      return {...state, inputs, isDirty: true, isValid: false}
    }

    case 'VALUE_BLURRED': {
      const thresholds = state.thresholds.map(threshold =>
        threshold.id === action.id
          ? {...threshold, value: parseFloat(state.inputs[action.id])}
          : threshold
      )

      const errors = validateThresholds(thresholds)

      const isValid = Object.values(errors).length === 0

      return {...state, thresholds, errors, isValid}
    }

    case 'THRESHOLD_ADDED': {
      const newThreshold = addThreshold(state.thresholds)

      const thresholds = sortThresholds([...state.thresholds, newThreshold])

      const inputs = {
        ...state.inputs,
        [newThreshold.id]: String(newThreshold.value),
      }

      return {...state, thresholds, inputs, isDirty: true}
    }

    case 'THRESHOLD_REMOVED': {
      const thresholds = state.thresholds.filter(
        threshold => threshold.id !== action.id
      )

      return {...state, thresholds, isDirty: true}
    }

    default:
      const unknownAction: never = action
      const unknownActionType = (unknownAction as any).type

      throw new Error(
        `unhandled action of type "${unknownActionType}" in ThresholdsSettings`
      )
  }
}

const ThresholdsSettings: FunctionComponent<Props> = ({
  thresholds,
  onSetThresholds,
}) => {
  const initialState: State = useMemo(
    () => ({
      thresholds: sortThresholds(
        cloneDeep(thresholds.filter(({type}) => type !== 'scale'))
      ),
      inputs: thresholds.reduce(
        (acc, {id, value}) => ({...acc, [id]: String(value)}),
        {}
      ),
      errors: {},
      isDirty: false,
      isValid: true,
    }),
    [thresholds]
  )

  const [state, dispatch] = useOneWayReducer(reducer, initialState)

  useEffect(() => {
    if (state.isDirty && state.isValid) {
      onSetThresholds(state.thresholds)
    }
  }, [state])

  return (
    <FlexBox
      direction={FlexDirection.Column}
      alignItems={AlignItems.Stretch}
      margin={ComponentSize.Medium}
      testID="threshold-settings"
    >
      <Button
        shape={ButtonShape.StretchToFit}
        icon={IconFont.Plus}
        text="Add a Threshold"
        onClick={() => dispatch({type: 'THRESHOLD_ADDED'})}
      />
      {state.thresholds.map(threshold => {
        const onChangeValue = value =>
          dispatch({
            type: 'VALUE_CHANGED',
            id: threshold.id,
            value,
          })

        const onChangeColor = (name, hex) =>
          dispatch({
            type: 'COLOR_CHANGED',
            id: threshold.id,
            name,
            hex,
          })

        const onRemove = () =>
          dispatch({
            type: 'THRESHOLD_REMOVED',
            id: threshold.id,
          })

        const onBlur = () =>
          dispatch({
            type: 'VALUE_BLURRED',
            id: threshold.id,
          })

        return (
          <ThresholdSetting
            key={threshold.id}
            id={threshold.id}
            name={threshold.name}
            type={threshold.type}
            value={state.inputs[threshold.id]}
            error={state.errors[threshold.id]}
            onBlur={onBlur}
            onRemove={onRemove}
            onChangeValue={onChangeValue}
            onChangeColor={onChangeColor}
          />
        )
      })}
    </FlexBox>
  )
}

export default ThresholdsSettings
