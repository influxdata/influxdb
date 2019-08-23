// Components
import LevelTableField from 'src/alerting/components/LevelTableField'
import TimeTableField from 'src/alerting/components/TimeTableField'
import CheckTableField from 'src/alerting/components/CheckTableField'
import NotificationRuleTableField from 'src/alerting/components/NotificationRuleTableField'
import NotificationEndpointTableField from 'src/alerting/components/NotificationEndpointTableField'
import SentTableField from 'src/alerting/components/SentTableField'

// Types
import {Fields} from 'src/eventViewer/types'

export const STATUS_FIELDS: Fields = [
  {
    rowKey: 'time',
    columnName: 'Time',
    columnWidth: 160,
    component: TimeTableField,
  },
  {
    rowKey: 'level',
    columnName: 'Level',
    columnWidth: 50,
    component: LevelTableField,
  },
  {
    rowKey: 'checkID',
    columnName: 'Check',
    columnWidth: 150,
    component: CheckTableField,
  },
  {
    rowKey: 'message',
    columnName: 'Message',
    columnWidth: 300,
  },
]

export const NOTIFICATION_FIELDS: Fields = [
  {
    rowKey: 'time',
    columnName: 'Time',
    columnWidth: 160,
    component: TimeTableField,
  },
  {
    rowKey: 'level',
    columnName: 'Level',
    columnWidth: 50,
    component: LevelTableField,
  },
  {
    rowKey: 'checkID',
    columnName: 'Check',
    columnWidth: 150,
    component: CheckTableField,
  },
  {
    rowKey: 'notificationRuleID',
    columnName: 'Notification Rule',
    columnWidth: 200,
    component: NotificationRuleTableField,
  },
  {
    rowKey: 'notificationEndpointID',
    columnName: 'Notification Endpoint',
    columnWidth: 200,
    component: NotificationEndpointTableField,
  },
  {
    rowKey: 'sent',
    columnName: 'Sent',
    columnWidth: 50,
    component: SentTableField,
  },
]

export const STATUS_BUCKET = 'fake_status_bucket'

export const NOTIFICATION_BUCKET = 'fake_notification_bucket'

export const EXAMPLE_STATUS_SEARCHES = [
  '"check" == "my check"',
  '"level" != "ok"',
  '"level" == "warn"',
  '"level" == "crit"',
  '"message" =~ /exceeded capacity/',
  '"check" == "my check" and ("level" == "crit" or "level" == "warn")',
]

export const EXAMPLE_NOTIFICATION_SEARCHES = [
  '"check" == "my check"',
  '"level" == "crit"',
  '"level" != "ok"',
  '"notification rule" == "my rule"',
]
