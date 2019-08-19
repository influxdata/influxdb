// Libraries
import React, {FC} from 'react'

// Components
import {
  FlexBox,
  FlexDirection,
  AlignItems,
  ComponentSize,
} from '@influxdata/clockface'
import ThresholdCondition from 'src/alerting/components/builder/ThresholdCondition'

// Types
import {ThresholdCheck} from 'src/types'

interface Props {
  check: Partial<ThresholdCheck>
}

const ThresholdConditions: FC<Props> = ({check}) => {
  const thresholds = {}
  if (check.thresholds) {
    check.thresholds.forEach(t => {
      thresholds[t.level] = t
    })
  }
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

export default ThresholdConditions
