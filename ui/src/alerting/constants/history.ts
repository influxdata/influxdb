// Components
import LevelTableField from 'src/alerting/components/LevelTableField'
import TagsTableField from 'src/alerting/components/TagsTableField'
import TimeTableField from 'src/alerting/components/TimeTableField'
import CheckTableField from 'src/alerting/components/CheckTableField'
import NotificationRuleTableField from 'src/alerting/components/NotificationRuleTableField'
import NotificationEndpointTableField from 'src/alerting/components/NotificationEndpointTableField'
import SentTableField from 'src/alerting/components/SentTableField'

// Types
import {FieldComponents} from 'src/eventViewer/types'

export const STATUS_FIELDS = ['time', 'level', 'check', 'message', 'tags']

export const STATUS_FIELD_COMPONENTS: FieldComponents = {
  time: TimeTableField,
  level: LevelTableField,
  tags: TagsTableField,
  check: CheckTableField,
}

export const STATUS_FIELD_WIDTHS = {
  time: 160,
  check: 150,
  message: 300,
  level: 50,
  tags: 300,
}

export const NOTIFICATION_FIELDS = [
  'time',
  'level',
  'check',
  'notificationRule',
  'notificationEndpoint',
  'sent',
]

export const NOTIFICATION_FIELD_COMPONENTS: FieldComponents = {
  time: TimeTableField,
  level: LevelTableField,
  check: CheckTableField,
  notificationRule: NotificationRuleTableField,
  notificationEndpoint: NotificationEndpointTableField,
  sent: SentTableField,
}

export const NOTIFICATION_FIELD_WIDTHS = {
  time: 160,
  level: 50,
  check: 150,
  notificationRule: 200,
  notificationEndpoint: 200,
  sent: 50,
}
