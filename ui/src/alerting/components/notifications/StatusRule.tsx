// Libraries
import React, {FC, useContext} from 'react'

// Components
import PeriodCount from 'src/alerting/components/notifications/PeriodCount'
import StatusLevels from 'src/alerting/components/notifications/StatusLevels'
import {NewRuleDispatch} from 'src/alerting/components/notifications/NewRuleOverlay'

// Types
import {StatusRuleItem} from 'src/types'
import {IconFont} from '@influxdata/clockface'

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
      statusRule: {
        ...status,
        value: {
          ...status.value,
          [name]: value,
        },
      },
    })
  }

  const onDelete = () => {
    dispatch({
      type: 'DELETE_STATUS_RULE',
      statusRuleID: status.id,
    })
  }

  return (
    <div className="condition-row" data-testid="status-rule">
      <div
        style={{
          position: 'absolute',
          right: '0',
          cursor: 'pointer',
        }}
        onClick={onDelete}
      >
        <span className={`icon ${IconFont.Remove}`} />
      </div>
      <PeriodCount period={period} count={count} onChange={onChange} />
      <StatusLevels status={status} />
    </div>
  )
}

export default StatusRuleComponent
