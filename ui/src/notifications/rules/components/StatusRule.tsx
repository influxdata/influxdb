// Libraries
import React, {FC} from 'react'

// Components
import {Panel, ComponentSize} from '@influxdata/clockface'
import StatusLevels from 'src/notifications/rules/components/StatusLevels'

// Types
import {StatusRuleDraft} from 'src/types'

interface Props {
  status: StatusRuleDraft
}

const StatusRuleComponent: FC<Props> = ({status}) => {
  return (
    <Panel testID="status-rule">
      <Panel.Body size={ComponentSize.ExtraSmall}>
        <StatusLevels status={status} />
      </Panel.Body>
    </Panel>
  )
}

export default StatusRuleComponent
