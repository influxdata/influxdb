// Libraries
import React, {FC} from 'react'

// Components
import {Panel, ComponentSize} from '@influxdata/clockface'
import StatusLevels from 'src/alerting/components/notifications/StatusLevels'

// Types
import {StatusRuleDraft} from 'src/types'

interface Props {
  status: StatusRuleDraft
}

const StatusRuleComponent: FC<Props> = ({status}) => {
  return (
    <Panel size={ComponentSize.ExtraSmall} testID="status-rule">
      <Panel.Body>
        <StatusLevels status={status} />
      </Panel.Body>
    </Panel>
  )
}

export default StatusRuleComponent
