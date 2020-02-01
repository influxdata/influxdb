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
  Label,
  Check as GenCheck,
  ThresholdCheck as GenThresholdCheck,
  DeadmanCheck as GenDeadmanCheck,
  CustomCheck as GenCustomCheck,
  NotificationRule as GenRule,
  NotificationEndpoint as GenEndpoint,
  TaskStatusType,
  Threshold,
  CheckBase as GenCheckBase,
  NotificationEndpointBase as GenEndpointBase,
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
  endpointStatus: TaskStatusType
}
export type GenEndpoint = GenEndpoint
export type NotificationEndpoint =
  | Omit<SlackNotificationEndpoint, 'status'> & EndpointOverrides
  | Omit<PagerDutyNotificationEndpoint, 'status'> & EndpointOverrides
  | Omit<HTTPNotificationEndpoint, 'status'> & EndpointOverrides
export type NotificationEndpointBase = GenEndpointBase & EndpointOverrides

/* Rule */
type RuleOverrides = {status: RemoteDataState; ruleStatus: TaskStatusType}

export type GenRule = GenRule
export type NotificationRule = GenRule & RuleOverrides

export type StatusRuleDraft = WithClientID<StatusRule>

export type TagRuleDraft = WithClientID<TagRule>

export type NotificationRuleBaseDraft = Overwrite<
  NotificationRuleBase,
  {
    id?: string
    status: RemoteDataState
    ruleStatus: TaskStatusType
    statusRules: StatusRuleDraft[]
    tagRules: TagRuleDraft[]
    labels?: Label[]
  }
>

type RuleDraft = SlackRule | SMTPRule | PagerDutyRule | HTTPRule

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
type CheckOverrides = {status: RemoteDataState; checkStatus: TaskStatusType}
export type CheckBase = Omit<GenCheckBase, 'status'> & CheckOverrides

export type GenCheck = GenCheck

export type ThresholdCheck = Omit<GenThresholdCheck, 'status'> & CheckOverrides

export type DeadmanCheck = Omit<GenDeadmanCheck, 'status'> & CheckOverrides

export type CustomCheck = Omit<GenCustomCheck, 'status'> & CheckOverrides

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
} from '../client'
