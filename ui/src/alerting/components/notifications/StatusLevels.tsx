// Libraries
import React, {FC, useContext} from 'react'

// Components
import LevelsDropdown from 'src/alerting/components/notifications/LevelsDropdown'
import StatusChangeDropdown from 'src/alerting/components/notifications/StatusChangeDropdown'
import {NewRuleDispatch} from './NewRuleOverlay'

// Types
import {StatusRuleItem} from 'src/types'

interface Props {
  status: StatusRuleItem
}

const StatusLevels: FC<Props> = ({status}) => {
  const {currentLevel, previousLevel} = status.value
  const dispatch = useContext(NewRuleDispatch)

  const onClickLevel = (type, level) => {
    const value = {
      ...status.value,
      [type]: {
        ...status.value[type],
        level,
      },
    }

    dispatch({
      type: 'UPDATE_STATUS_RULES',
      statusRule: {
        ...status,
        value,
      },
    })
  }

  return (
    <div className="status-levels--container">
      <div className="sentence-frag">When status</div>
      <StatusChangeDropdown status={status} />
      {!!previousLevel && (
        <LevelsDropdown
          type="previousLevel"
          selectedLevel={previousLevel.level}
          onClickLevel={onClickLevel}
        />
      )}
      {!!previousLevel && <div className="sentence-frag">to</div>}
      <LevelsDropdown
        type="currentLevel"
        selectedLevel={currentLevel.level}
        onClickLevel={onClickLevel}
      />
    </div>
  )
}

export default StatusLevels
