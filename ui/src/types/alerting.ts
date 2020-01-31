import {
  StatusRule,
  NotificationRuleBase,
  TagRule,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  HTTPNotificationRuleBase,
  Label,
  ThresholdCheck,
  DeadmanCheck,
  CustomCheck,
  NotificationRule as GenRule,
  NotificationEndpoint as GenEndpoint,
  NotificationEndpointBase as GenBaseEndpoint,
} from 'src/client'

type Omit<T, U> = Pick<T, Exclude<keyof T, U>>
type Overwrite<T, U> = Omit<T, keyof U> & U

interface WithClientID<T> {
  cid: string
  value: T
}

export type NotificationEndpoint = GenEndpoint & {
  loadingStatus: RemoteDataState
}

export type NotificationEndpointBase = GenBaseEndpoint & {
  loadingStatus: RemoteDataState
}

export type NotificationRule = GenRule & {
  loadingStatus: RemoteDataState
}

export type StatusRuleDraft = WithClientID<StatusRule>

export type TagRuleDraft = WithClientID<TagRule>

export type NotificationRuleBaseDraft = Overwrite<
  NotificationRuleBase,
  {
    id?: string
    statusRules: StatusRuleDraft[]
    tagRules: TagRuleDraft[]
    labels?: Label[]
  }
>

type RuleDraft = SlackRule | SMTPRule | PagerDutyRule | HTTPRule

export type NotificationRuleDraft = RuleDraft & {loadingStatus: RemoteDataState}

type SlackRule = NotificationRuleBaseDraft & SlackNotificationRuleBase
type SMTPRule = NotificationRuleBaseDraft & SMTPNotificationRuleBase
type PagerDutyRule = NotificationRuleBaseDraft & PagerDutyNotificationRuleBase
type HTTPRule = NotificationRuleBaseDraft & HTTPNotificationRuleBase

export type LowercaseCheckStatusLevel =
  | 'crit'
  | 'warn'
  | 'info'
  | 'ok'
  | 'unknown'

// The data for a row in the status history table
export interface StatusRow {
  time: number
  level: LowercaseCheckStatusLevel
  checkID: string
  checkName: string
  message: string
}

// The data for a row in the notification history table
export interface NotificationRow {
  time: number
  level: LowercaseCheckStatusLevel
  checkID: string
  checkName: string
  notificationRuleID: string
  notificationRuleName: string
  notificationEndpointID: string
  notificationEndpointName: string
  sent: 'true' | 'false' // See https://github.com/influxdata/idpe/issues/4645
}

export {
  Threshold,
  CheckBase,
  StatusRule,
  TagRule,
  PostCheck,
  CheckStatusLevel,
  RuleStatusLevel,
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
  ThresholdCheck,
  DeadmanCheck,
  CustomCheck,
  PostNotificationEndpoint,
  NotificationRuleBase,
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
  PostNotificationRule,
  CheckPatch,
} from '../client'

import {Threshold, HTTPNotificationEndpoint} from '../client'
import {RemoteDataState} from '@influxdata/clockface'

export type Check = ThresholdCheck | DeadmanCheck | CustomCheck

export type CheckType = Check['type']

export type ThresholdType = Threshold['type']

export type CheckTagSet = ThresholdCheck['tags'][0]

export type AlertHistoryType = 'statuses' | 'notifications'

export type HTTPMethodType = HTTPNotificationEndpoint['method']
export type HTTPAuthMethodType = HTTPNotificationEndpoint['authMethod']
