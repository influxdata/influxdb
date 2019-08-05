import {StatusRule, NotificationRule, TagRule} from 'src/client'

export interface AddID<T> {
  id: string
  value: T
}

export type StatusRuleItem = AddID<StatusRule>
export type TagRuleItem = AddID<TagRule>

type ExcludeKeys<T> = Pick<T, Exclude<keyof T, 'statusRules' | 'tagRules'>>

export interface NotificationRuleBox extends ExcludeKeys<NotificationRule> {
  schedule: 'cron' | 'every'
  statusRules: StatusRuleItem[]
  tagRules: TagRuleItem[]
}

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
