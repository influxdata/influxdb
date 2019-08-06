// Libraries
import React, {FC} from 'react'

// Components
import {Panel} from '@influxdata/clockface'
import StatusLevels from 'src/alerting/components/notifications/StatusLevels'

// Types
import {StatusRuleItem} from 'src/types'

interface Props {
  status: StatusRuleItem
}

const StatusRuleComponent: FC<Props> = ({status}) => {
  return (
    <Panel>
      <Panel.Body>
        <StatusLevels status={status} />
      </Panel.Body>
    </Panel>
  )
}

export default StatusRuleComponent
