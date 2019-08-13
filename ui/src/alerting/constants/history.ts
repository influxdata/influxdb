// Components
import LevelTableField from 'src/alerting/components/LevelTableField'
import TagsTableField from 'src/alerting/components/TagsTableField'
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
    rowKey: 'check',
    columnName: 'Check',
    columnWidth: 150,
    component: CheckTableField,
  },
  {
    rowKey: 'message',
    columnName: 'Message',
    columnWidth: 300,
  },
  {
    rowKey: 'tags',
    columnName: 'Tags',
    columnWidth: 300,
    component: TagsTableField,
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
    rowKey: 'check',
    columnName: 'Check',
    columnWidth: 150,
    component: CheckTableField,
  },
  {
    rowKey: 'notificationRule',
    columnName: 'Notification Rule',
    columnWidth: 200,
    component: NotificationRuleTableField,
  },
  {
    rowKey: 'notificationEndpoint',
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
