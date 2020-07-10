import {ThresholdCheck, TagRuleDraft, NotificationEndpoint} from 'src/types'
import {CheckStatusLevel} from 'src/client'
import {
  ComponentColor,
  InfluxColors,
  RemoteDataState,
} from '@influxdata/clockface'

export const DEFAULT_CHECK_NAME = 'Name this Check'
export const DEFAULT_NOTIFICATION_RULE_NAME = 'Name this Notification Rule'

export const CHECK_NAME_MAX_LENGTH = 68
export const DEFAULT_CHECK_EVERY = '1m'
export const DEFAULT_CHECK_OFFSET = '0s'
export const DEFAULT_CHECK_TAGS = []
export const DEFAULT_CHECK_REPORT_ZERO = false
export const DEFAULT_DEADMAN_LEVEL: CheckStatusLevel = 'CRIT'
export const DEFAULT_STATUS_MESSAGE =
  'Check: ${ r._check_name } is: ${ r._level }'

export const CHECK_OFFSET_OPTIONS = [
  '0s',
  '5s',
  '15s',
  '1m',
  '5m',
  '15m',
  '1h',
  '6h',
  '12h',
  '24h',
  '2d',
  '7d',
  '30d',
]

export const MONITORING_BUCKET = '_monitoring'

export const LEVEL_COLORS = {
  OK: InfluxColors.Viridian,
  INFO: InfluxColors.Ocean,
  WARN: InfluxColors.Thunder,
  CRIT: InfluxColors.Fire,
  UNKNOWN: InfluxColors.Amethyst,
}

export const LEVEL_COMPONENT_COLORS = {
  OK: ComponentColor.Success,
  INFO: ComponentColor.Primary,
  WARN: ComponentColor.Warning,
  CRIT: ComponentColor.Danger,
}

export const DEFAULT_THRESHOLD_CHECK: Partial<ThresholdCheck> = {
  name: DEFAULT_CHECK_NAME,
  type: 'threshold',
  activeStatus: 'active',
  thresholds: [],
  every: DEFAULT_CHECK_EVERY,
  offset: DEFAULT_CHECK_OFFSET,
  statusMessageTemplate: DEFAULT_STATUS_MESSAGE,
}

export const NEW_TAG_RULE_DRAFT: TagRuleDraft = {
  cid: '',
  value: {
    key: '',
    value: '',
    operator: 'equal',
  },
}

export const DEFAULT_ENDPOINT_URLS = {
  slack: 'https://hooks.slack.com/services/X/X/X',
  pagerduty: 'https://events.pagerduty.com/v2/enqueue',
  http: 'https://www.example.com/endpoint',
  teams: 'https://office.outlook.com/hook/XXXX',
}

export const NEW_ENDPOINT_DRAFT: NotificationEndpoint = {
  name: 'Name this Endpoint',
  description: '',
  activeStatus: 'active',
  type: 'slack',
  token: '',
  url: DEFAULT_ENDPOINT_URLS['slack'],
  status: RemoteDataState.Done,
  labels: [],
}

export const NEW_ENDPOINT_FIXTURES: NotificationEndpoint[] = [
  {
    id: '1',
    orgID: '1',
    userID: '1',
    description: 'interrupt everyone at work',
    name: 'Slack',
    activeStatus: 'active',
    type: 'slack',
    url: 'insert.slack.url.here',
    token: 'plerps',
    status: RemoteDataState.Done,
    labels: [],
  },
  {
    id: '3',
    orgID: '1',
    userID: '1',
    description: 'interrupt someone by all means known to man',
    name: 'PagerDuty',
    activeStatus: 'active',
    type: 'pagerduty',
    clientURL: 'insert.pagerduty.client.url.here',
    routingKey: 'plerps',
    status: RemoteDataState.Done,
    labels: [],
  },
]
