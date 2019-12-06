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
import {useCheckYDomain} from 'src/alerting/utils/vis'
import {getVisTable} from 'src/timeMachine/selectors'
import {
  removeThreshold,
  updateThreshold,
} from 'src/alerting/actions/alertBuilder'

// Types
import {
  AppState,
  Threshold,
  ThresholdType,
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
  CheckStatusLevel,
} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'
import {Table} from '@influxdata/giraffe'
import {LEVEL_COMPONENT_COLORS} from 'src/alerting/constants'

interface StateProps {
  table: Table
}

interface DispatchProps {
  onUpdateThreshold: typeof updateThreshold
  onRemoveThreshold: typeof removeThreshold
}

interface OwnProps {
  threshold: Threshold
  level: CheckStatusLevel
}

type Props = StateProps & DispatchProps & OwnProps

const defaultThreshold = {
  type: 'greater' as 'greater',
}

const ThresholdCondition: FC<Props> = ({
  level,
  table,
  threshold,
  onUpdateThreshold,
  onRemoveThreshold,
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

  const [yDomain] = useCheckYDomain(table.getColumn('_value', 'number'), [])

  const addLevel = () => {
    const low = yDomain[0] || 0
    const high = yDomain[1] || 40
    const newThreshold = {
      ...defaultThreshold,
      value: (high - low) / 2 + low,
      level,
    }
    onUpdateThreshold(newThreshold)
  }

  const removeLevel = () => {
    onRemoveThreshold(level)
  }

  const changeValue = (value: number) => {
    const newThreshold = {...threshold, value} as
      | GreaterThreshold
      | LesserThreshold

    onUpdateThreshold(newThreshold)
  }

  const changeRange = (min: number, max: number) => {
    const newThreshold = {...threshold, min, max} as RangeThreshold

    onUpdateThreshold(newThreshold)
  }

  const changeThresholdType = (toType: ThresholdType, within?: boolean) => {
    if (toType === 'greater' || toType === 'lesser') {
      const valueThreshold = {
        type: toType,
        level: threshold.level,
        value: inputs[0],
      } as GreaterThreshold | LesserThreshold
      onUpdateThreshold(valueThreshold)
    }
    if (toType === 'range') {
      const rangeThreshold = {
        type: toType,
        level: threshold.level,
        min: inputs[0],
        max: inputs[1],
        within,
      } as RangeThreshold
      onUpdateThreshold(rangeThreshold)
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

const mstp = (state: AppState): StateProps => {
  const giraffeResult = getVisTable(state)

  return {
    table: giraffeResult.table,
  }
}

const mdtp: DispatchProps = {
  onUpdateThreshold: updateThreshold,
  onRemoveThreshold: removeThreshold,
}

export {ThresholdCondition}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(ThresholdCondition)
