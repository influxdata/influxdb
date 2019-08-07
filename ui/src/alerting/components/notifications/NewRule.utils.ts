// Libraries
import {omit} from 'lodash'

// Types
import {
  StatusRuleDraft,
  LevelRule,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  NotificationEndpoint,
} from 'src/types'

type EndpointBase =
  | SlackNotificationRuleBase
  | SMTPNotificationRuleBase
  | PagerDutyNotificationRuleBase

export const getEndpointBase = (
  endpoints: NotificationEndpoint[],
  id: string
): EndpointBase => {
  const endpoint = endpoints.find(e => e.id === id)

  switch (endpoint.type) {
    case 'slack': {
      return {messageTemplate: '', channel: '', type: 'slack'}
    }
    case 'smtp': {
      return {to: '', bodyTemplate: '', subjectTemplate: '', type: 'smtp'}
    }
    case 'pagerduty': {
      return {messageTemplate: '', type: 'pagerduty'}
    }

    default: {
      throw new Error('Unknown endpoint type in <RuleMessage />')
    }
  }
}

type Change = 'changes from' | 'equal'
export const CHANGES: Change[] = ['changes from', 'equal']

export const activeChange = (status: StatusRuleDraft) => {
  const {currentLevel, previousLevel} = status.value

  if (!!previousLevel) {
    return 'changes from'
  }

  if (currentLevel.operation === 'equal') {
    return 'equal'
  }

  throw new Error(
    'Changed statusRule.currentLevel.operation to unknown operator'
  )
}

export const previousLevel: LevelRule = {level: 'OK'}
export const changeStatusRule = (
  status: StatusRuleDraft,
  change: Change
): StatusRuleDraft => {
  if (change === 'equal') {
    return omit(status, 'value.previousLevel') as StatusRuleDraft
  }

  const {value} = status
  const newValue = {...value, previousLevel}

  return {...status, value: newValue}
}
