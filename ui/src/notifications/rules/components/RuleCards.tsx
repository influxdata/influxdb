// Libraries
import React, {FC} from 'react'

// Components
import NotificationRuleCard from 'src/notifications/rules/components/RuleCard'
import {EmptyState, ResourceList} from '@influxdata/clockface'
import FilterList from 'src/shared/components/Filter'

// Types
import {NotificationRuleDraft} from 'src/types'
import {ComponentSize} from '@influxdata/clockface'

interface Props {
  rules: NotificationRuleDraft[]
  searchTerm: string
}

const NotificationRuleCards: FC<Props> = ({rules, searchTerm}) => {
  const cards = rules =>
    rules.map(nr => <NotificationRuleCard key={nr.id} rule={nr} />)

  const filteredCards = (
    <FilterList<NotificationRuleDraft>
      list={rules}
      searchKeys={['name']}
      searchTerm={searchTerm}
    >
      {filtered => (
        <ResourceList.Body
          emptyState={<EmptyNotificationRulesList searchTerm={searchTerm} />}
        >
          {cards(filtered)}
        </ResourceList.Body>
      )}
    </FilterList>
  )

  return <ResourceList>{filteredCards}</ResourceList>
}

const EmptyNotificationRulesList: FC<{searchTerm: string}> = ({searchTerm}) => {
  if (searchTerm) {
    return (
      <EmptyState size={ComponentSize.Small} className="alert-column--empty">
        <EmptyState.Text>
          No <b>rules</b> match your search
        </EmptyState.Text>
      </EmptyState>
    )
  }

  return (
    <EmptyState size={ComponentSize.Small} className="alert-column--empty">
      <EmptyState.Text>
        You need at least 1 <b>Notification Endpoint</b> before
        <br />
        you can create a <b>Notification Rule</b>
      </EmptyState.Text>
    </EmptyState>
  )
}

export default NotificationRuleCards
