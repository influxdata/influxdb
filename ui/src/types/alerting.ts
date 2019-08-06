import {StatusRule, NotificationRuleBase, TagRule} from 'src/client'

export interface AddID<T> {
  id: string
  value: T
}

export type StatusRuleItem = AddID<StatusRule>
export type TagRuleItem = AddID<TagRule>

type ExcludeKeys<T> = Pick<T, Exclude<keyof T, 'statusRules' | 'tagRules'>>

export interface NotificationRuleBaseBox
  extends ExcludeKeys<NotificationRuleBase> {
  schedule: 'cron' | 'every'
  statusRules: StatusRuleItem[]
  tagRules: TagRuleItem[]
}

export type NotificationRuleBox = SlackRule | SMTPRule | PagerDutyRule

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
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
  DeadmanCheck,
  NotificationEndpoint,
  NotificationRuleBase,
  NotificationRule,
  SMTPNotificationRule,
  SlackNotificationRule,
  PagerDutyNotificationRule,
} from '../client'
