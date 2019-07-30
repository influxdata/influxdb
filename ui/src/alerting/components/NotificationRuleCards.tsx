// Libraries
import React, {FunctionComponent} from 'react'

// Components
import NotificationRuleCard from 'src/alerting/components/NotificationRuleCard'
import {EmptyState, ResourceList} from '@influxdata/clockface'

// Types
import {NotificationRule} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  notificationRules: NotificationRule[]
}

const NotificationRuleCards: FunctionComponent<Props> = ({
  notificationRules,
}) => {
  return (
    <>
      <ResourceList>
        <ResourceList.Body emptyState={<EmptyNotificationRulesList />}>
          {notificationRules.map(nr => (
            <NotificationRuleCard key={nr.id} notificationRule={nr} />
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
