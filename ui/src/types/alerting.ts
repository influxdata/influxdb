export {
  Check,
  CheckType,
  CheckBaseTags,
  CheckBase,
  CheckStatusLevel,
  ThresholdCheck,
  ThresholdType,
  GreaterThreshold,
  LesserThreshold,
  RangeThreshold,
  DeadmanCheck,
  NotificationRuleType,
  CheckThreshold,
  NotificationRuleBase,
} from '@influxdata/influx'

import {
  SlackNotificationRule as SlackNotificationRuleAPI,
  SMTPNotificationRule as SMTPNotificationRuleAPI,
  PagerDutyNotificationRule as PagerDutyNotificationRuleAPI,
} from '@influxdata/influx'

export interface SlackNotificationRule extends SlackNotificationRuleAPI {
  id: string
}

export interface SMTPNotificationRule extends SMTPNotificationRuleAPI {
  id: string
}

export interface PagerDutyNotificationRule
  extends PagerDutyNotificationRuleAPI {
  id: string
}

export type NotificationRule =
  | SlackNotificationRule
  | SMTPNotificationRule
  | PagerDutyNotificationRule
