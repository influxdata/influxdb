// Libraries
import React, {FunctionComponent} from 'react'

// Components
import TimeMachineAlertBuilder from 'src/alerting/components/builder/AlertBuilder'
import {
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'
import CustomizeCheckQueryButton from 'src/timeMachine/components/CustomizeCheckQueryButton'
import HelpButton from 'src/alerting/components/builder/HelpButton'
import RawDataToggle from 'src/timeMachine/components/RawDataToggle'
import {isFlagEnabled} from 'src/shared/utils/featureFlag'

const TimeMachineAlerting: FunctionComponent = () => {
  return (
    <div className="time-machine-queries">
      <div className="time-machine-queries--controls">
        <div className="time-machine--editor-title">Configure a Check</div>
        <div className="time-machine-queries--buttons">
          <FlexBox
            direction={FlexDirection.Row}
            justifyContent={JustifyContent.FlexEnd}
            margin={ComponentSize.Small}
          >
            {isFlagEnabled('customCheckQuery') && <CustomizeCheckQueryButton />}
            <RawDataToggle />
            <HelpButton />
          </FlexBox>
        </div>
      </div>
      <div className="time-machine-queries--body">
        <TimeMachineAlertBuilder />
      </div>
    </div>
  )
}

export default TimeMachineAlerting
