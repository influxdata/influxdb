// Libraries
import React, {FC} from 'react'
import {connect} from 'react-redux'

// Components
import {ResourceCard} from '@influxdata/clockface'

// Types
import {NotificationRule, AppState, NotificationEndpoint} from 'src/types'

interface OwnProps {
  rule: NotificationRule
}

interface StateProps {
  endpoints: NotificationEndpoint[]
}

type Props = OwnProps & StateProps

const MatchingRuleCard: FC<Props> = ({rule, endpoints}) => {
  const endpoint = endpoints.find(e => e.id === rule.endpointID)

  return (
    <ResourceCard
      key={`rule-id--${rule.id}`}
      testID="rule-card"
      name={<ResourceCard.Name name={rule.name} />}
      description={<ResourceCard.Description description={rule.description} />}
      metaData={[
        <>{`Checks every: ${rule.every}`}</>,
        <>{`Sends notifications to: ${endpoint.name}`}</>,
      ]}
    />
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    endpoints: {list: endpoints},
  } = state
  return {
    endpoints,
  }
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(MatchingRuleCard)
