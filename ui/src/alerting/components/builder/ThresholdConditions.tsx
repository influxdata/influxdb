// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {
  FlexBox,
  FlexDirection,
  AlignItems,
  ComponentSize,
} from '@influxdata/clockface'
import ThresholdCondition from 'src/alerting/components/builder/ThresholdCondition'

// Actions
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {Threshold, AppState} from 'src/types'

interface ThresholdsObject {
  [k: string]: Threshold
}

interface StateProps {
  thresholds: ThresholdsObject
}

const ThresholdConditions: FC<StateProps> = ({thresholds}) => {
  return (
    <FlexBox
      direction={FlexDirection.Column}
      alignItems={AlignItems.Stretch}
      margin={ComponentSize.Medium}
    >
      <ThresholdCondition level="OK" threshold={thresholds['OK']} />
      <ThresholdCondition level="INFO" threshold={thresholds['INFO']} />
      <ThresholdCondition level="WARN" threshold={thresholds['WARN']} />
      <ThresholdCondition level="CRIT" threshold={thresholds['CRIT']} />
    </FlexBox>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    alerting: {check},
  } = getActiveTimeMachine(state)

  const thresholds = {}
  if (check.type === 'threshold') {
    check.thresholds.forEach(t => {
      thresholds[t.level] = t
    })
  }

  return {thresholds}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(ThresholdConditions)
