// Libraries
import React, {FC} from 'react'

// Components
import LevelsDropdown from 'src/alerting/components/notifications/LevelsDropdown'
import StatusChangeDropdown from 'src/alerting/components/notifications/StatusChangeDropdown'

// Types
import {StatusRuleItem} from 'src/types'

interface Props {
  status: StatusRuleItem
}

const StatusLevels: FC<Props> = ({status}) => {
  const {currentLevel, previousLevel} = status.value

  return (
    <div className="status-levels--container">
      <div className="sentence-frag">When status</div>
      <StatusChangeDropdown status={status} />
      {!!previousLevel && <LevelsDropdown level={previousLevel.level} />}
      {!!previousLevel && <div className="sentence-frag">to</div>}
      <LevelsDropdown level={currentLevel.level} />
    </div>
  )
}

export default StatusLevels
