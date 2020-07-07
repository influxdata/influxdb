// Libraries
import React, {FC} from 'react'
import {connect, ConnectedProps} from 'react-redux'

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
import {AppState} from 'src/types'
import DurationInput from 'src/shared/components/DurationInput'
import {CHECK_OFFSET_OPTIONS} from 'src/alerting/constants'

type ReduxProps = ConnectedProps<typeof connector>
type Props = ReduxProps

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

const mstp = ({alertBuilder: {staleTime, timeSince, level}}: AppState) => ({
  staleTime,
  timeSince,
  level,
})

const mdtp = {
  onSetStaleTime: setStaleTime,
  onSetTimeSince: setTimeSince,
  onSetLevel: setLevel,
}

const connector = connect(mstp, mdtp)

export default connector(DeadmanConditions)
