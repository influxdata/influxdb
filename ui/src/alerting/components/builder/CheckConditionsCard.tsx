// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {
  FlexBox,
  FlexDirection,
  AlignItems,
  ComponentSize,
  Radio,
  JustifyContent,
} from '@influxdata/clockface'
import ThresholdConditions from 'src/alerting/components/builder/ThresholdConditions'
import DeadmanConditions from 'src/alerting/components/builder/DeadmanConditions'
import BuilderCard from 'src/timeMachine/components/builderCard/BuilderCard'

// Actions & Selectors
import {changeCheckType} from 'src/timeMachine/actions'
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// Types
import {Check, AppState} from 'src/types'

interface StateProps {
  check: Partial<Check>
}

interface DispatchProps {
  changeCheckType: typeof changeCheckType
}

type Props = DispatchProps & StateProps

const CheckConditionsCard: FC<Props> = ({check, changeCheckType}) => {
  return (
    <BuilderCard
      testID="builder-conditions"
      className="alert-builder--card alert-builder--conditions-card"
    >
      <BuilderCard.Header title="Conditions" />
      <BuilderCard.Body addPadding={true} autoHideScrollbars={true}>
        <FlexBox
          direction={FlexDirection.Row}
          alignItems={AlignItems.Center}
          stretchToFitWidth={true}
          justifyContent={JustifyContent.Center}
          className="alert-builder--check-type-selector"
        >
          <Radio>
            <Radio.Button
              key="threshold"
              id="threshold"
              titleText="threshold"
              value="threshold"
              active={check.type === 'threshold'}
              onClick={changeCheckType}
            >
              Threshold
            </Radio.Button>
            <Radio.Button
              key="deadman"
              id="deadman"
              titleText="deadman"
              value="deadman"
              active={check.type === 'deadman'}
              onClick={changeCheckType}
            >
              Deadman
            </Radio.Button>
          </Radio>
        </FlexBox>
        <FlexBox
          direction={FlexDirection.Column}
          alignItems={AlignItems.Stretch}
          margin={ComponentSize.Medium}
        >
          {check.type === 'deadman' ? (
            <DeadmanConditions check={check} />
          ) : (
            <ThresholdConditions check={check} />
          )}
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

const mdtp: DispatchProps = {
  changeCheckType: changeCheckType,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(CheckConditionsCard)
