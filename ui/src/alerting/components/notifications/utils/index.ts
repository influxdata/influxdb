// Libraries
import {omit} from 'lodash'
import uuid from 'uuid'

// Types
import {
  StatusRule,
  TagRule,
  StatusRuleDraft,
  LevelRule,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  NotificationEndpoint,
  NotificationRule,
  NotificationRuleDraft,
  NewNotificationRule,
} from 'src/types'

type RuleVariantFields =
  | SlackNotificationRuleBase
  | SMTPNotificationRuleBase
  | PagerDutyNotificationRuleBase

export const getRuleVariantDefaults = (
  endpoints: NotificationEndpoint[],
  id: string
): RuleVariantFields => {
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
      throw new Error(`Could not find NotificationEndpoint with id "${id}"`)
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

export const initRuleDraft = (orgID: string): NotificationRuleDraft => ({
  type: 'slack',
  every: '10m',
  orgID,
  name: '',
  status: 'active',
  messageTemplate: '',
  tagRules: [],
  statusRules: [
    {
      cid: '',
      value: {
        currentLevel: {operation: 'equal', level: 'WARN'},
        previousLevel: {operation: 'equal', level: 'OK'},
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

  const tagRules: TagRule[] = draftRule.tagRules.map(r => r.value)

  return {
    ...draftRule,
    statusRules,
    tagRules,
  } as NotificationRule
}

// Prepare a persisted rule for editing
export const ruleToDraftRule = (
  rule: NotificationRule
): NotificationRuleDraft => {
  return {
    ...rule,
    statusRules: rule.statusRules.map(value => ({cid: uuid.v4(), value})),
    tagRules: rule.tagRules.map(value => ({cid: uuid.v4(), value})),
  }
}
