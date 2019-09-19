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
import ThresholdConditions from 'src/alerting/components/builder/ThresholdConditions'
import DeadmanConditions from 'src/alerting/components/builder/DeadmanConditions'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

// Actions & Selectors
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {Check, AppState} from 'src/types'

interface StateProps {
  check: Partial<Check>
}

type Props = StateProps

const CheckConditionsCard: FC<Props> = ({check}) => {
  let cardTitle: string
  let conditionsComponent: JSX.Element

  if (check.type === 'deadman') {
    cardTitle = 'Deadman'
    conditionsComponent = <DeadmanConditions check={check} />
  }

  if (check.type === 'threshold') {
    cardTitle = 'Thresholds'
    conditionsComponent = <ThresholdConditions check={check} />
  }

  return (
    <BuilderCard
      testID="builder-conditions"
      className="alert-builder--card alert-builder--conditions-card"
    >
      <BuilderCard.Header title={cardTitle} />
      <BuilderCard.Body addPadding={true} autoHideScrollbars={true}>
        <FlexBox
          direction={FlexDirection.Column}
          alignItems={AlignItems.Stretch}
          margin={ComponentSize.Medium}
        >
          {conditionsComponent}
        </FlexBox>
      </BuilderCard.Body>
    </BuilderCard>
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
