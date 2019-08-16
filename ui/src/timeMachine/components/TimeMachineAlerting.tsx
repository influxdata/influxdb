// Libraries
import React, {FunctionComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineAlertBuilder from 'src/alerting/components/builder/AlertBuilder'
import {
  ComponentSize,
  FlexBox,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'
import RemoveButton from 'src/alerting/components/builder/RemoveButton'
import HelpButton from 'src/alerting/components/builder/HelpButton'

// Types
import {AppState} from 'src/types'

interface StateProps {
  isInVEO: boolean
}

const TimeMachineAlerting: FunctionComponent<StateProps> = ({isInVEO}) => {
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
            <HelpButton />
            {isInVEO && <RemoveButton />}
          </FlexBox>
        </div>
      </div>
      <div className="time-machine-queries--body">
        <TimeMachineAlertBuilder />
      </div>
    </div>
  )
}

const mstp = (state: AppState) => {
  const isInVEO = state.timeMachines.activeTimeMachineID === 'veo'

  return {isInVEO}
}

export default connect<StateProps>(mstp)(TimeMachineAlerting)
