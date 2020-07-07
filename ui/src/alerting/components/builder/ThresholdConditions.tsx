// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

// Components
import ThresholdCondition from 'src/alerting/components/builder/ThresholdCondition'

// Types
import {Threshold, AppState} from 'src/types'

interface StateProps {
  thresholds: {[k: string]: Threshold}
}

const ThresholdConditions: FC<StateProps> = ({thresholds}) => {
  return (
    <>
      <ThresholdCondition level="CRIT" threshold={thresholds['CRIT']} />
      <ThresholdCondition level="WARN" threshold={thresholds['WARN']} />
      <ThresholdCondition level="INFO" threshold={thresholds['INFO']} />
      <ThresholdCondition level="OK" threshold={thresholds['OK']} />
    </>
  )
}

const mstp = ({alertBuilder: {thresholds: thresholdsArray}}: AppState) => {
  const thresholds = {}
  thresholdsArray.forEach(t => {
    thresholds[t.level] = t
  })
  return {thresholds}
}

export default connect<StateProps, {}, {}>(mstp, null)(ThresholdConditions)
