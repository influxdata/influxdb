import {
  StatusRule,
  NotificationRuleBase,
  TagRule,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  HTTPNotificationRuleBase,
  NotificationRule,
  CheckStatusLevel,
} from 'src/client'

type Omit<T, U> = Pick<T, Exclude<keyof T, U>>
type Overwrite<T, U> = Omit<T, keyof U> & U

interface WithClientID<T> {
  cid: string
  value: T
}

export type StatusRuleDraft = WithClientID<StatusRule>

export type TagRuleDraft = WithClientID<TagRule>

// TODO: Spec this out in the OpenAPI spec instead. It should be whatever the
// server accepts as the request body for a `POST /api/v2/notificationRules`
export type NewNotificationRule = Omit<NotificationRule, 'id'>

export type NotificationRuleBaseDraft = Overwrite<
  NotificationRuleBase,
  {
    id?: string
    statusRules: StatusRuleDraft[]
    tagRules: TagRuleDraft[]
  }
>

export type NotificationRuleDraft =
  | SlackRule
  | SMTPRule
  | PagerDutyRule
  | HTTPRule

type SlackRule = NotificationRuleBaseDraft & SlackNotificationRuleBase
type SMTPRule = NotificationRuleBaseDraft & SMTPNotificationRuleBase
type PagerDutyRule = NotificationRuleBaseDraft & PagerDutyNotificationRuleBase
type HTTPRule = NotificationRuleBaseDraft & HTTPNotificationRuleBase

// The data for a row in the status history table
export interface StatusRow {
  time: number
  level: CheckStatusLevel
  checkID: string
  checkName: string
  message: string
}

// The data for a row in the notification history table
export interface NotificationRow {
  time: number
  level: CheckStatusLevel
  checkID: string
  checkName: string
  notificationRuleID: string
  notificationRuleName: string
  notificationEndpointID: string
  notificationEndpointName: string
  sent: 'true' | 'false' // See https://github.com/influxdata/idpe/issues/4645
}

export {
  Check,
  Threshold,
  CheckBase,
  StatusRule,
  TagRule,
  CheckStatusLevel,
  RuleStatusLevel,
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
  ThresholdCheck,
  DeadmanCheck,
  NotificationEndpoint,
  NotificationRuleBase,
  NotificationRule,
  NotificationRuleUpdate,
  NotificationEndpointType,
  SMTPNotificationRuleBase,
  SlackNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  HTTPNotificationRuleBase,
  SMTPNotificationRule,
  SlackNotificationRule,
  PagerDutyNotificationRule,
  HTTPNotificationRule,
  PagerDutyNotificationEndpoint,
  SlackNotificationEndpoint,
  HTTPNotificationEndpoint,
  NotificationEndpointUpdate,
} from '../client'

import {Check, Threshold, HTTPNotificationEndpoint} from '../client'

export type CheckType = Check['type']
export type ThresholdType = Threshold['type']

export type CheckTagSet = Check['tags'][0]

export type AlertHistoryType = 'statuses' | 'notifications'

export type HTTPMethodType = HTTPNotificationEndpoint['method']
export type HTTPAuthMethodType = HTTPNotificationEndpoint['authMethod']
