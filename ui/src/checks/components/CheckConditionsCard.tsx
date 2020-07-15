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

// Types
import {CheckType, AppState} from 'src/types'

interface StateProps {
  checkType: CheckType
}

type Props = StateProps

const CheckConditionsCard: FC<Props> = ({checkType}) => {
  let cardTitle: string
  let conditionsComponent: JSX.Element

  if (checkType === 'deadman') {
    cardTitle = 'Deadman'
    conditionsComponent = <DeadmanConditions />
  } else if (checkType === 'threshold') {
    cardTitle = 'Thresholds'
    conditionsComponent = <ThresholdConditions />
  } else {
    throw new Error('Incorrect check type provided to <CheckConditionsCard/>')
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

const mstp = ({alertBuilder: {type}}: AppState) => {
  return {checkType: type}
}

export default connect<StateProps, {}, {}>(mstp, null)(CheckConditionsCard)
