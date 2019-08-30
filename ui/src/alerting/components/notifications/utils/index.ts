// Libraries
import {omit} from 'lodash'
import uuid from 'uuid'

// Types
import {
  StatusRule,
  TagRule,
  StatusRuleDraft,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  NotificationEndpoint,
  NotificationRule,
  NotificationRuleDraft,
  NewNotificationRule,
  HTTPNotificationRuleBase,
  RuleStatusLevel,
} from 'src/types'

type RuleVariantFields =
  | SlackNotificationRuleBase
  | SMTPNotificationRuleBase
  | PagerDutyNotificationRuleBase
  | HTTPNotificationRuleBase

export const getRuleVariantDefaults = (
  endpoints: NotificationEndpoint[],
  id: string
): RuleVariantFields => {
  const endpoint = endpoints.find(e => e.id === id)

  switch (endpoint.type) {
    case 'slack': {
      return {messageTemplate: '', channel: '', type: 'slack'}
    }

    case 'pagerduty': {
      return {messageTemplate: '', type: 'pagerduty'}
    }

    case 'http': {
      return {type: 'http', url: ''}
    }

    default: {
      throw new Error(`Could not find NotificationEndpoint with id "${id}"`)
    }
  }
}

type Change = 'changes from' | 'is equal to'
export const CHANGES: Change[] = ['changes from', 'is equal to']

export const activeChange = (status: StatusRuleDraft) => {
  const {previousLevel} = status.value

  if (!!previousLevel) {
    return 'changes from'
  }
  return 'is equal to'
}

export const previousLevel = 'OK' as RuleStatusLevel

export const changeStatusRule = (
  status: StatusRuleDraft,
  changeType: Change
): StatusRuleDraft => {
  if (changeType === 'is equal to') {
    return omit(status, 'value.previousLevel') as StatusRuleDraft
  }

  const {value} = status
  const newValue = {...value, previousLevel}

  return {...status, value: newValue}
}

export const initRuleDraft = (orgID: string): NotificationRuleDraft => ({
  type: 'http',
  every: '10m',
  url: '',
  orgID,
  name: '',
  status: 'active',
  endpointID: '',
  tagRules: [
    {
      cid: uuid.v4(),
      value: {key: '', value: '', operator: 'equal'},
    },
  ],
  statusRules: [
    {
      cid: uuid.v4(),
      value: {
        currentLevel: 'CRIT',
        period: '1h',
        count: 1,
      },
    },
  ],
  description: '',
})

// Prepare a newly created rule for persistence
export const draftRuleToNewRule = (
  draftRule: NotificationRuleDraft
): NewNotificationRule => {
  return {
    ...draftRule,
    statusRules: draftRule.statusRules.map(r => r.value),
    tagRules: draftRule.tagRules.map(r => r.value),
  }
}

// Prepare an edited rule for persistence
export const draftRuleToRule = (
  draftRule: NotificationRuleDraft
): NotificationRule => {
  if (!draftRule.id) {
    throw new Error('draft rule is missing id')
  }

  const statusRules: StatusRule[] = draftRule.statusRules.map(r => r.value)

  const tagRules: TagRule[] = draftRule.tagRules
    .map(r => r.value)
    .filter(tr => tr.key && tr.value)

  return {
    ...draftRule,
    statusRules,
    tagRules,
  } as NotificationRule
}

export const newTagRuleDraft = () => ({
  cid: uuid.v4(),
  value: {
    key: '',
    value: '',
    operator: 'equal' as 'equal',
  },
})

// Prepare a persisted rule for editing
export const ruleToDraftRule = (
  rule: NotificationRule
): NotificationRuleDraft => {
  const statusRules = rule.statusRules || []
  const tagRules = rule.tagRules || []
  return {
    ...rule,
    statusRules: statusRules.map(value => ({cid: uuid.v4(), value})),
    tagRules: tagRules.map(value => ({cid: uuid.v4(), value})),
  }
}
