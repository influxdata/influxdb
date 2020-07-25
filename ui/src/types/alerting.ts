import {
  StatusRule,
  NotificationRuleBase,
  TagRule,
  SlackNotificationEndpoint,
  PagerDutyNotificationEndpoint,
  HTTPNotificationEndpoint,
  SlackNotificationRuleBase,
  SMTPNotificationRuleBase,
  PagerDutyNotificationRuleBase,
  HTTPNotificationRuleBase,
  Check as GCheck,
  ThresholdCheck as GThresholdCheck,
  DeadmanCheck as GDeadmanCheck,
  CustomCheck as GenCustomCheck,
  NotificationRule as GRule,
  NotificationEndpoint as GEndpoint,
  TaskStatusType,
  Threshold,
  CheckBase as GenCheckBase,
  NotificationEndpointBase as GenEndpointBase,
  TelegramNotificationRuleBase,
  TelegramNotificationEndpoint,
} from 'src/client'
import {RemoteDataState} from 'src/types'

type Omit<T, U> = Pick<T, Exclude<keyof T, U>>
type Overwrite<T, U> = Omit<T, keyof U> & U

interface WithClientID<T> {
  cid: string
  value: T
}

/* Endpoints */
type EndpointOverrides = {
  status: RemoteDataState
  activeStatus: TaskStatusType
  labels: string[]
}
// GenEndpoint is the shape of a NotificationEndpoint from the server -- before any UI specific fields are or modified
export type GenEndpoint = GEndpoint
export type NotificationEndpoint =
  | (Omit<SlackNotificationEndpoint, 'status' | 'labels'> & EndpointOverrides)
  | (Omit<PagerDutyNotificationEndpoint, 'status' | 'labels'> &
      EndpointOverrides)
  | (Omit<HTTPNotificationEndpoint, 'status' | 'labels'> & EndpointOverrides)
  | (Omit<TelegramNotificationEndpoint, 'status' | 'labels'> &
      EndpointOverrides)
export type NotificationEndpointBase = Omit<GenEndpointBase, 'labels'> &
  EndpointOverrides

/* Rule */
type RuleOverrides = {status: RemoteDataState; activeStatus: TaskStatusType}

// GenRule is the shape of a NotificationRule from the server -- before any UI specific fields are added or modified
export type GenRule = GRule
export type NotificationRule = GenRule & RuleOverrides

export type StatusRuleDraft = WithClientID<StatusRule>

export type TagRuleDraft = WithClientID<TagRule>

export type NotificationRuleBaseDraft = Overwrite<
  NotificationRuleBase,
  {
    id?: string
    status: RemoteDataState
    activeStatus: TaskStatusType
    statusRules: StatusRuleDraft[]
    tagRules: TagRuleDraft[]
    labels?: string[]
  }
>

type RuleDraft = SlackRule | SMTPRule | PagerDutyRule | HTTPRule | TelegramRule

export type NotificationRuleDraft = RuleDraft

type SlackRule = NotificationRuleBaseDraft &
  SlackNotificationRuleBase &
  RuleOverrides

type SMTPRule = NotificationRuleBaseDraft &
  SMTPNotificationRuleBase &
  RuleOverrides

type PagerDutyRule = NotificationRuleBaseDraft &
  PagerDutyNotificationRuleBase &
  RuleOverrides

type HTTPRule = NotificationRuleBaseDraft &
  HTTPNotificationRuleBase &
  RuleOverrides

type TelegramRule = NotificationRuleBaseDraft &
  TelegramNotificationRuleBase &
  RuleOverrides

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

/* Checks */
type CheckOverrides = {
  status: RemoteDataState
  activeStatus: TaskStatusType
  labels: string[]
}
export type CheckBase = Omit<GenCheckBase, 'status'> & CheckOverrides

// GenCheck is the shape of a Check from the server -- before UI specific properties are added
export type GenCheck = GCheck
export type GenThresholdCheck = GThresholdCheck
export type GenDeadmanCheck = GDeadmanCheck

export type ThresholdCheck = Omit<GenThresholdCheck, 'status' | 'labels'> &
  CheckOverrides

export type DeadmanCheck = Omit<GenDeadmanCheck, 'status' | 'labels'> &
  CheckOverrides

export type CustomCheck = Omit<GenCustomCheck, 'status' | 'labels'> &
  CheckOverrides

export type Check = ThresholdCheck | DeadmanCheck | CustomCheck

export type CheckType = Check['type']

export type ThresholdType = Threshold['type']

export type CheckTagSet = ThresholdCheck['tags'][0]

export type AlertHistoryType = 'statuses' | 'notifications'

export type HTTPMethodType = HTTPNotificationEndpoint['method']
export type HTTPAuthMethodType = HTTPNotificationEndpoint['authMethod']

export {
  Threshold,
  StatusRule,
  TagRule,
  PostCheck,
  CheckStatusLevel,
  RuleStatusLevel,
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
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
  TaskStatusType,
  TelegramNotificationEndpoint,
  TelegramNotificationRuleBase,
  TelegramNotificationRule,
} from '../client'
