import {
  StatusRule,
  NotificationRuleBase,
  TagRule,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
} from 'src/client'

export interface AddID<T> {
  id: string
  value: T
}

export type StatusRuleDraft = AddID<StatusRule>
export type TagRuleDraft = AddID<TagRule>

type ExcludeKeys<T> = Pick<T, Exclude<keyof T, 'statusRules' | 'tagRules'>>

export interface NotificationRuleBaseDraft
  extends ExcludeKeys<NotificationRuleBase> {
  statusRules: StatusRuleDraft[]
  tagRules: TagRuleDraft[]
}

export type NotificationRuleDraft = SlackRule | SMTPRule | PagerDutyRule

type SlackRule = NotificationRuleBaseDraft & SlackNotificationRuleBase
type SMTPRule = NotificationRuleBaseDraft & SMTPNotificationRuleBase
type PagerDutyRule = NotificationRuleBaseDraft & PagerDutyNotificationRuleBase

export {
  Check,
  CheckBase,
  StatusRule,
  LevelRule,
  TagRule,
  CheckStatusLevel,
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
  ThresholdCheck,
  DeadmanCheck,
  NotificationEndpoint,
  NotificationRuleBase,
  NotificationRule,
  SMTPNotificationRuleBase,
  SlackNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  SMTPNotificationRule,
  SlackNotificationRule,
  PagerDutyNotificationRule,
} from '../client'

import {Check} from '../client'

export type CheckType = Check['type']
