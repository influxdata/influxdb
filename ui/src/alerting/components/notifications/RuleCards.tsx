// Libraries
import React, {FunctionComponent} from 'react'

// Components
import NotificationRuleCard from 'src/alerting/components/notifications/RuleCard'
import {EmptyState, ResourceList} from '@influxdata/clockface'

// Types
import {NotificationRuleDraft} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  rules: NotificationRuleDraft[]
}

const NotificationRuleCards: FunctionComponent<Props> = ({rules}) => {
  return (
    <>
      <ResourceList>
        <ResourceList.Body emptyState={<EmptyNotificationRulesList />}>
          {rules.map(nr => (
            <NotificationRuleCard key={nr.id} rule={nr} />
          ))}
        </ResourceList.Body>
      </ResourceList>
    </>
  )
}

const EmptyNotificationRulesList: FunctionComponent = () => {
  return (
    <EmptyState size={ComponentSize.ExtraSmall}>
      <EmptyState.Text
        text="Looks like you don’t have any Notification Rules, why not create one?"
        highlightWords={['Notification Rules']}
      />
    </EmptyState>
  )
}

export default NotificationRuleCards
