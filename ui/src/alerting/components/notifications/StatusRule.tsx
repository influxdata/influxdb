// Libraries
import React, {FC, useContext} from 'react'

// Components
import PeriodCount from 'src/alerting/components/notifications/PeriodCount'
import StatusLevels from 'src/alerting/components/notifications/StatusLevels'
import {NewRuleDispatch} from 'src/alerting/components/notifications/NewRuleOverlay'

// Types
import {StatusRuleItem} from 'src/types'

interface Props {
  status: StatusRuleItem
}

const StatusRuleComponent: FC<Props> = ({status}) => {
  const dispatch = useContext(NewRuleDispatch)
  const {period, count} = status.value

  const onChange = ({target}) => {
    const {name, value} = target

    dispatch({
      type: 'UPDATE_STATUS_RULES',
      status: {
        ...status,
        [name]: value,
      },
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
