// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {
  FlexBox,
  Panel,
  ComponentSize,
  PanelBody,
  TextBlock,
  InputType,
  Input,
  FlexDirection,
  InfluxColors,
} from '@influxdata/clockface'
import CheckLevelsDropdown from 'src/alerting/components/builder/CheckLevelsDropdown'

// Actions
import {updateTimeMachineCheck} from 'src/timeMachine/actions'

// Types
import {DeadmanCheck, CheckStatusLevel} from 'src/types'

interface DispatchProps {
  onUpdateTimeMachineCheck: typeof updateTimeMachineCheck
}

interface OwnProps {
  check: Partial<DeadmanCheck>
}

type Props = DispatchProps & OwnProps

const DeadmanConditions: FC<Props> = ({
  check: {staleTime, timeSince, level},
  onUpdateTimeMachineCheck,
}) => {
  const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    onUpdateTimeMachineCheck({[e.target.name]: e.target.value})
  }
  const handleChangeLevel = (level: CheckStatusLevel) => {
    onUpdateTimeMachineCheck({level})
  }
  return (
    <Panel backgroundColor={InfluxColors.Castle} testID="panel">
      <PanelBody testID="panel--body">
        <FlexBox
          direction={FlexDirection.Column}
          margin={ComponentSize.Small}
          testID="component-spacer"
        >
          <FlexBox
            direction={FlexDirection.Row}
            margin={ComponentSize.Small}
            stretchToFitWidth
            testID="component-spacer"
          >
            <FlexBox.Child testID="component-spacer--flex-child">
              <TextBlock
                testID="when-value-text-block"
                text="When values are not reporting"
              />
            </FlexBox.Child>
          </FlexBox>
          <FlexBox
            direction={FlexDirection.Row}
            margin={ComponentSize.Small}
            stretchToFitWidth
            testID="component-spacer"
          >
            <TextBlock testID="when-value-text-block" text="for" />
            <FlexBox.Child testID="component-spacer--flex-child">
              <Input
                onChange={handleChange}
                name="timeSince"
                testID="input-field"
                type={InputType.Text}
                value={timeSince}
              />
            </FlexBox.Child>
            <TextBlock testID="set-status-to-text-block" text="set status to" />
            <FlexBox.Child testID="component-spacer--flex-child">
              <CheckLevelsDropdown
                selectedLevel={level}
                onClickLevel={handleChangeLevel}
              />
            </FlexBox.Child>
          </FlexBox>
          <FlexBox
            direction={FlexDirection.Row}
            margin={ComponentSize.Small}
            stretchToFitWidth
            testID="component-spacer"
          >
            <TextBlock
              testID="when-value-text-block"
              text="And stop checking after"
            />
            <FlexBox.Child testID="component-spacer--flex-child">
              <Input
                onChange={handleChange}
                name="staleTime"
                testID="input-field"
                type={InputType.Text}
                value={staleTime}
              />
            </FlexBox.Child>
          </FlexBox>
        </FlexBox>
      </PanelBody>
    </Panel>
  )
}

const mdtp: DispatchProps = {
  onUpdateTimeMachineCheck: updateTimeMachineCheck,
}

export default connect<{}, DispatchProps, {}>(
  null,
  mdtp
)(DeadmanConditions)
