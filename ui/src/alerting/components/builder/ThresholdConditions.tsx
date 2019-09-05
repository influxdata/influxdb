// Libraries
import React, {FC} from 'react'

// Components
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
    <>
      <ThresholdCondition level="CRIT" threshold={thresholds['CRIT']} />
      <ThresholdCondition level="WARN" threshold={thresholds['WARN']} />
      <ThresholdCondition level="INFO" threshold={thresholds['INFO']} />
      <ThresholdCondition level="OK" threshold={thresholds['OK']} />
    </>
  )
}

export default ThresholdConditions
