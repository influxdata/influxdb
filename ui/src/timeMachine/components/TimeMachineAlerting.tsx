// Libraries
import React, {FunctionComponent} from 'react'

// Components
import TimeMachineAlertBuilder from 'src/alerting/components/builder/AlertBuilder'
import {
  ComponentSize,
  ComponentSpacer,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'
import RemoveButton from 'src/alerting/components/builder/RemoveButton'
import HelpButton from 'src/alerting/components/builder/HelpButton'
import {TimeMachineIDs} from 'src/timeMachine/constants'

interface Props {
  activeTimeMachineID: TimeMachineIDs
}

const TimeMachineAlerting: FunctionComponent<Props> = ({
  activeTimeMachineID,
}) => {
  return (
    <div className="time-machine-queries">
      <div className="time-machine-queries--controls">
        Check Builder
        <div className="time-machine-queries--buttons">
          <ComponentSpacer
            direction={FlexDirection.Row}
            justifyContent={JustifyContent.FlexEnd}
            margin={ComponentSize.Small}
          >
            <HelpButton />
            {activeTimeMachineID == 'veo' && <RemoveButton />}
          </ComponentSpacer>
        </div>
      </div>
      <div className="time-machine-queries--body">
        <TimeMachineAlertBuilder />
      </div>
    </div>
  )
}

export default TimeMachineAlerting
