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
        text="A Notification  Rule  will query statuses written by Checks  to determine if a notification should be sent to a Notification  Endpoint"
        highlightWords={['Notification', 'Rule', 'Endpoint', 'Checks']}
      />
      <br />
      <a href="#" target="_blank">
        Documentation
      </a>
    </EmptyState>
  )
}

export default NotificationRuleCards
