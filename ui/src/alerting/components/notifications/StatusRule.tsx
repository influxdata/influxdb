// Libraries
import React, {FC, Dispatch} from 'react'

// Components
import PeriodCount from 'src/alerting/components/notifications/PeriodCount'
import StatusLevels from 'src/alerting/components/notifications/StatusLevels'

// Types
import {StatusRuleItem} from 'src/types'
import {Actions} from 'src/alerting/components/notifications/NewRuleOverlay.reducer'

interface Props {
  status: StatusRuleItem
  dispatch: Dispatch<Actions>
}

const StatusRuleComponent: FC<Props> = ({status, dispatch}) => {
  const {period, count} = status.value
  const onChange = ({target}) => {
    const {name, value} = target
    const newStatus = {
      ...status,
      [name]: value,
    }

    dispatch({
      type: 'UPDATE_STATUS_RULES',
      status: newStatus,
    })
  }

  return (
    <div className="condition-row">
      <PeriodCount period={period} count={count} onChange={onChange} />
      <StatusLevels status={status} />
    </div>
  )
}

export default StatusRuleComponent
