import {
  Check,
  DashboardQuery,
  ThresholdCheck,
  TagRuleDraft,
  DeadmanCheck,
} from 'src/types'
import {NotificationEndpoint} from 'src/client'
import {ComponentColor} from '@influxdata/clockface'

export const DEFAULT_CHECK_NAME = 'Name this check'
export const DEFAULT_NOTIFICATION_RULE_NAME = 'Name this notification rule'

export const CHECK_NAME_MAX_LENGTH = 68
export const DEFAULT_CHECK_CRON = '1h'
export const DEFAULT_CHECK_EVERY = '1h'
export const DEFAULT_CHECK_OFFSET = '0s'
export const DEFAULT_CHECK_REPORT_ZERO = false

export const LEVEL_COLORS = {
  OK: '#32B08C',
  INFO: '#4591ED',
  WARN: '#FFD255',
  CRIT: '#DC4E58',
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
  status: 'active',
  thresholds: [],
  every: DEFAULT_CHECK_EVERY,
  offset: DEFAULT_CHECK_OFFSET,
}

export const DEFAULT_DEADMAN_CHECK: Partial<DeadmanCheck> = {
  name: DEFAULT_CHECK_NAME,
  type: 'deadman',
  status: 'active',
  every: DEFAULT_CHECK_EVERY,
  offset: DEFAULT_CHECK_OFFSET,
  reportZero: DEFAULT_CHECK_REPORT_ZERO,
}

export const CHECK_QUERY_FIXTURE: DashboardQuery = {
  text: 'this is query',
  editMode: 'advanced',
  builderConfig: null,
  name: 'great q',
}

export const CHECK_FIXTURE_1: Check = {
  id: '1',
  type: 'threshold',
  name: 'Amoozing check',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  query: CHECK_QUERY_FIXTURE,
  status: 'active',
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: 'CRIT',
      type: 'lesser',
      value: 10,
    },
  ],
}

export const CHECK_FIXTURE_2: Check = {
  id: '2',
  type: 'threshold',
  name: 'Another check',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  query: CHECK_QUERY_FIXTURE,
  status: 'active',
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: 'INFO',
      type: 'greater',
      value: 10,
    },
  ],
}

export const CHECK_FIXTURES: Array<Check> = [CHECK_FIXTURE_1, CHECK_FIXTURE_2]

export const NEW_TAG_RULE_DRAFT: TagRuleDraft = {
  cid: '',
  value: {
    key: '',
    value: '',
    operator: 'equal',
  },
}

export const NEW_ENDPOINT_DRAFT: NotificationEndpoint = {
  name: 'HTTP Endpoint',
  method: 'POST',
  authMethod: 'none',
  description: '',
  status: 'active',
  type: 'http',
  token: '',
  url: '',
}

export const NEW_ENDPOINT_FIXTURES: NotificationEndpoint[] = [
  {
    id: '1',
    orgID: '1',
    userID: '1',
    description: 'interrupt everyone at work',
    name: 'Slack',
    status: 'active',
    type: 'slack',
    url: 'insert.slack.url.here',
    token: 'plerps',
  },
  {
    id: '3',
    orgID: '1',
    userID: '1',
    description: 'interrupt someone by all means known to man',
    name: 'PagerDuty',
    status: 'active',
    type: 'pagerduty',
    url: 'insert.pagerduty.url.here',
    routingKey: 'plerps',
  },
]
