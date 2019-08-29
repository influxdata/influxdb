// Libraries
import React, {FunctionComponent, useState, useEffect} from 'react'
import {connect} from 'react-redux'
import {get} from 'lodash'

// Components
import MatchingRuleCard from 'src/alerting/components/builder/MatchingRuleCard'

// Actions
import {getActiveTimeMachine} from 'src/timeMachine/selectors'

// API
import * as api from 'src/client'

//Types
import {NotificationRule, Check, AppState, CheckTagSet} from 'src/types'
import {EmptyState, ComponentSize} from '@influxdata/clockface'

interface StateProps {
  check: Partial<Check>
  orgID: string
}

const CheckMatchingRulesCard: FunctionComponent<StateProps> = ({
  check,
  orgID,
}) => {
  const getMatchingRules = async (): Promise<NotificationRule[]> => {
    const tags: CheckTagSet[] = get(check, 'tags', [])
    const tagString = tags.map(t => `tag=${t.key}:${t.value}`).join('&')

    // todo also: get tags from query results
    const resp = await api.getNotificationRules({
      query: {orgID, tag: tagString},
    })

    if (resp.status !== 200) {
      // throw new Error(resp.data.message)
      return
    }

    setMatchingRules(resp.data.notificationRules)
  }

  const [matchingRules, setMatchingRules] = useState<NotificationRule[]>([])
  useEffect(() => {
    getMatchingRules()
  }, [check.tags])

  if (matchingRules.length === 0) {
    return (
      <EmptyState
        size={ComponentSize.Small}
        className="alert-builder--card__empty"
      >
        <EmptyState.Text text="Notification Rules configured to act on tag sets matching this Alert Check will automatically show up here" />
        <EmptyState.Text text="Looks like no notification rules match the tag set defined in this Alert Check" />
      </EmptyState>
    )
  }
  return (
    <>
      {matchingRules.map(r => (
        <MatchingRuleCard key={r.id} rule={r} />
      ))}
    </>
  )
}

const mstp = (state: AppState): StateProps => {
  const {
    orgs: {
      org: {id: orgID},
    },
  } = state
  const {
    alerting: {check},
  } = getActiveTimeMachine(state)

  return {check, orgID}
}

export default connect<StateProps, {}, {}>(
  mstp,
  null
)(CheckMatchingRulesCard)
