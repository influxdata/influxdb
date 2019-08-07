import {StatusRule, NotificationRuleBase, TagRule} from 'src/client'

export interface AddID<T> {
  id: string
  value: T
}

export type StatusRuleDraft = AddID<StatusRule>
export type TagRuleDraft = AddID<TagRule>

type ExcludeKeys<T> = Pick<T, Exclude<keyof T, 'statusRules' | 'tagRules'>>

export interface NotificationRuleBaseBox
  extends ExcludeKeys<NotificationRuleBase> {
  schedule: 'cron' | 'every'
  statusRules: StatusRuleDraft[]
  tagRules: TagRuleDraft[]
}

export type NotificationRuleDraft = SlackRule | SMTPRule | PagerDutyRule

export type SlackBase = {
  type: 'slack'
  channel?: string
  messageTemplate: string
}

type SlackRule = NotificationRuleBaseBox & SlackBase

export type SMTPBase = {
  type: 'smtp'
  to: string
  bodyTemplate?: string
  subjectTemplate: string
}

type SMTPRule = NotificationRuleBaseBox & SMTPBase

export type PagerDutyBase = {
  type: 'pagerduty'
  messageTemplate: string
}

type PagerDutyRule = NotificationRuleBaseBox & PagerDutyBase

export {
  Check,
  CheckBase,
  StatusRule,
  LevelRule,
  TagRule,
  CheckStatusLevel,
  ThresholdCheck,
  DeadmanCheck,
  NotificationEndpoint,
  NotificationRuleBase,
  NotificationRule,
  SMTPNotificationRule,
  SlackNotificationRule,
  PagerDutyNotificationRule,
} from '../client'

import {Check} from '../client'

export type CheckType = Check['type']
