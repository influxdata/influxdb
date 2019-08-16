// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import DeadmanConditions from 'src/alerting/components/builder/DeadmanConditions'
import ThresholdConditions from 'src/alerting/components/builder/ThresholdConditions'

// Types
import {Check, AppState} from 'src/types'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

interface StateProps {
  check: Partial<Check>
}

const CheckConditionsCard: FC<StateProps> = ({check}) => {
  return check.type === 'deadman' ? (
    <DeadmanConditions />
  ) : (
    <ThresholdConditions check={check} />
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    alerting: {check},
  } = getActiveTimeMachine(state)

  return {check}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(CheckConditionsCard)
