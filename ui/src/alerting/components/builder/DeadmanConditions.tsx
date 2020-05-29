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
  FlexDirection,
  InfluxColors,
} from '@influxdata/clockface'
import CheckLevelsDropdown from 'src/checks/components/CheckLevelsDropdown'

// Actions
import {
  setStaleTime,
  setTimeSince,
  setLevel,
} from 'src/alerting/actions/alertBuilder'

// Types
import {CheckStatusLevel, AppState} from 'src/types'
import DurationInput from 'src/shared/components/DurationInput'
import {CHECK_OFFSET_OPTIONS} from 'src/alerting/constants'

interface DispatchProps {
  onSetStaleTime: typeof setStaleTime
  onSetTimeSince: typeof setTimeSince
  onSetLevel: typeof setLevel
}

interface StateProps {
  staleTime: string
  timeSince: string
  level: CheckStatusLevel
}

type Props = DispatchProps & StateProps

const DeadmanConditions: FC<Props> = ({
  staleTime,
  timeSince,
  level,
  onSetStaleTime,
  onSetTimeSince,
  onSetLevel,
}) => {
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
              <DurationInput
                suggestions={CHECK_OFFSET_OPTIONS}
                onSubmit={onSetTimeSince}
                value={timeSince}
                showDivider={false}
              />
            </FlexBox.Child>
            <TextBlock testID="set-status-to-text-block" text="set status to" />
            <FlexBox.Child testID="component-spacer--flex-child">
              <CheckLevelsDropdown
                selectedLevel={level}
                onClickLevel={onSetLevel}
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
              <DurationInput
                suggestions={CHECK_OFFSET_OPTIONS}
                onSubmit={onSetStaleTime}
                value={staleTime}
                showDivider={false}
              />
            </FlexBox.Child>
          </FlexBox>
        </FlexBox>
      </PanelBody>
    </Panel>
  )
}

const mstp = ({
  alertBuilder: {staleTime, timeSince, level},
}: AppState): StateProps => ({staleTime, timeSince, level})

const mdtp: DispatchProps = {
  onSetStaleTime: setStaleTime,
  onSetTimeSince: setTimeSince,
  onSetLevel: setLevel,
}

export default connect<StateProps, DispatchProps, {}>(
  mstp,
  mdtp
)(DeadmanConditions)
