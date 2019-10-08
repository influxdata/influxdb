// Libraries
import React, {FC, useState, useEffect} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import DashedButton from 'src/shared/components/dashed_button/DashedButton'
import ThresholdStatement from 'src/alerting/components/builder/ThresholdStatement'
import ThresholdValueInput from 'src/alerting/components/builder/ThresholdValueInput'
import ThresholdRangeInput from 'src/alerting/components/builder/ThresholdRangeInput'

// Actions
import {
  updateCheckThreshold,
  removeCheckThreshold,
} from 'src/timeMachine/actions'

// Types
import {
  Threshold,
  ThresholdType,
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
  CheckStatusLevel,
} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'
import {LEVEL_COMPONENT_COLORS} from 'src/alerting/constants'

interface DispatchProps {
  onUpdateCheckThreshold: typeof updateCheckThreshold
  onRemoveCheckThreshold: typeof removeCheckThreshold
}

interface OwnProps {
  threshold: Threshold
  level: CheckStatusLevel
}

type Props = DispatchProps & OwnProps

const defaultThreshold = {
  type: 'greater' as 'greater',
  value: 20,
}

const ThresholdCondition: FC<Props> = ({
  level,
  threshold,
  onUpdateCheckThreshold,
  onRemoveCheckThreshold,
}) => {
  const [inputs, changeInputs] = useState([
    get(threshold, 'value') || get(threshold, 'min', 0),
    get(threshold, 'max', 100),
  ])

  useEffect(() => {
    changeInputs([
      get(threshold, 'value') || get(threshold, 'min', inputs[0]),
      get(threshold, 'max', inputs[1]),
    ])
  }, [threshold])

  const addLevel = () => {
    const newThreshold = {...defaultThreshold, level}
    onUpdateCheckThreshold(newThreshold)
  }

  const removeLevel = () => {
    onRemoveCheckThreshold(level)
  }

  const changeValue = (value: number) => {
    const newThreshold = {...threshold, value} as
      | GreaterThreshold
      | LesserThreshold

    onUpdateCheckThreshold(newThreshold)
  }

  const changeRange = (min: number, max: number) => {
    const newThreshold = {...threshold, min, max} as RangeThreshold

    onUpdateCheckThreshold(newThreshold)
  }

  const changeThresholdType = (toType: ThresholdType, within?: boolean) => {
    if (toType === 'greater' || toType === 'lesser') {
      const valueThreshold = {
        type: toType,
        level: threshold.level,
        value: inputs[0],
      } as GreaterThreshold | LesserThreshold
      onUpdateCheckThreshold(valueThreshold)
    }
    if (toType === 'range') {
      const rangeThreshold = {
        type: toType,
        level: threshold.level,
        min: inputs[0],
        max: inputs[1],
        within,
      } as RangeThreshold
      onUpdateCheckThreshold(rangeThreshold)
    }
  }

  if (!threshold) {
    return (
      <DashedButton
        text={`+ ${level}`}
        color={LEVEL_COMPONENT_COLORS[level]}
        size={ComponentSize.Large}
        onClick={addLevel}
        testID={`add-threshold-condition-${level}`}
      />
    )
  }
  return (
    <ThresholdStatement
      threshold={threshold}
      removeLevel={removeLevel}
      changeThresholdType={changeThresholdType}
    >
      {threshold.type === 'range' ? (
        <ThresholdRangeInput threshold={threshold} changeRange={changeRange} />
      ) : (
        <ThresholdValueInput threshold={threshold} changeValue={changeValue} />
      )}
    </ThresholdStatement>
  )
}

const mdtp: DispatchProps = {
  onUpdateCheckThreshold: updateCheckThreshold,
  onRemoveCheckThreshold: removeCheckThreshold,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(ThresholdCondition)
