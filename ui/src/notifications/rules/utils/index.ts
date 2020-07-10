// Libraries
import {omit} from 'lodash'
import uuid from 'uuid'

// Types
import {
  StatusRuleDraft,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  TelegramNotificationRuleBase,
  NotificationEndpoint,
  NotificationRuleDraft,
  HTTPNotificationRuleBase,
  RuleStatusLevel,
  PostNotificationRule,
  GenRule,
  TeamsNotificationRuleBase,
} from 'src/types'
import {RemoteDataState} from '@influxdata/clockface'

type RuleVariantFields =
  | SlackNotificationRuleBase
  | SMTPNotificationRuleBase
  | PagerDutyNotificationRuleBase
  | HTTPNotificationRuleBase
  | TelegramNotificationRuleBase
  | TeamsNotificationRuleBase

const defaultMessage =
  'Notification Rule: ${ r._notification_rule_name } triggered by check: ${ r._check_name }: ${ r._message }'

export const getRuleVariantDefaults = (
  endpoints: NotificationEndpoint[],
  id: string
): RuleVariantFields => {
  const endpoint = endpoints.find(e => e.id === id)

  switch (endpoint.type) {
    case 'slack': {
      return {messageTemplate: defaultMessage, channel: '', type: 'slack'}
    }

    case 'pagerduty': {
      return {messageTemplate: defaultMessage, type: 'pagerduty'}
    }

    case 'http': {
      return {type: 'http', url: ''}
    }

    case 'telegram': {
      // wrap all variable values into `` to prevent telegram's markdown errors
      const messageTemplate = defaultMessage.replace(
        /\$\{[^\}]+\}/g,
        x => `\`${x}\``
      )
      return {
        messageTemplate: messageTemplate,
        parseMode: 'MarkdownV2',
        disableWebPagePreview: false,
        type: 'telegram',
      }
    }
    
    case 'teams': {
      return {
        messageTemplate: defaultMessage,
        title: '${ r._notification_rule_name }',
        type: 'teams',
      }
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
  offset: '0s',
  url: '',
  orgID,
  name: '',
  activeStatus: 'active',
  status: RemoteDataState.NotStarted,
  endpointID: '',
  tagRules: [],
  labels: [],
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
export const draftRuleToPostRule = (
  draftRule: NotificationRuleDraft
): PostNotificationRule => {
  return {
    ...draftRule,
    status: draftRule.activeStatus,
    statusRules: draftRule.statusRules.map(r => r.value),
    tagRules: draftRule.tagRules
      .map(r => r.value)
      .filter(tr => tr.key && tr.value),
  } as PostNotificationRule
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
export const ruleToDraftRule = (rule: GenRule): NotificationRuleDraft => {
  const statusRules = rule.statusRules || []
  const tagRules = rule.tagRules || []
  return {
    ...rule,
    labels: rule.labels.map(l => l.id),
    status: RemoteDataState.Done,
    activeStatus: rule.status,
    offset: rule.offset || '',
    statusRules: statusRules.map(value => ({cid: uuid.v4(), value})),
    tagRules: tagRules.map(value => ({cid: uuid.v4(), value})),
  }
}
