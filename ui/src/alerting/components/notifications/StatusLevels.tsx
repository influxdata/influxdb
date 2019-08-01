// Libraries
import React, {FC, Dispatch} from 'react'

// Components
import LevelsDropdown from 'src/alerting/components/notifications/LevelsDropdown'
import StatusChangeDropdown from 'src/alerting/components/notifications/StatusChangeDropdown'

// Types
import {StatusRuleItem} from 'src/types'
import {Actions} from 'src/alerting/components/notifications/NewRuleOverlay.reducer'

interface Props {
  status: StatusRuleItem
  dispatch: Dispatch<Actions>
}

const StatusLevels: FC<Props> = ({status, dispatch}) => {
  const {currentLevel, previousLevel} = status.value

  return (
    <div className="status-levels--container">
      <div className="sentence-frag">When status</div>
      <StatusChangeDropdown status={status} dispatch={dispatch} />
      {!!previousLevel && <LevelsDropdown level={previousLevel.level} />}
      {!!previousLevel && <div className="sentence-frag">to</div>}
      <LevelsDropdown level={currentLevel.level} />
    </div>
  )
}

export default StatusLevels
