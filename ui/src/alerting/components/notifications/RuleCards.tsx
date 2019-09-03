// Libraries
import React, {FC} from 'react'

// Components
import NotificationRuleCard from 'src/alerting/components/notifications/RuleCard'
import {EmptyState, ResourceList} from '@influxdata/clockface'

// Types
import {NotificationRuleDraft} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  rules: NotificationRuleDraft[]
}

const NotificationRuleCards: FC<Props> = ({rules}) => {
  return (
    <ResourceList>
      <ResourceList.Body emptyState={<EmptyNotificationRulesList />}>
        {rules.map(nr => (
          <NotificationRuleCard key={nr.id} rule={nr} />
        ))}
      </ResourceList.Body>
    </ResourceList>
  )
}

const EmptyNotificationRulesList: FC = () => {
  return (
    <EmptyState size={ComponentSize.Small} className="alert-column--empty">
      <EmptyState.Text
        text="You need at least 1 Notification  Endpoint  before LINEBREAK you can create a Notification  Rule"
        highlightWords={['Notification', 'Rule', 'Endpoint']}
      />
    </EmptyState>
  )
}

export default NotificationRuleCards
