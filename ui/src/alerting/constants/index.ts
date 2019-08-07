import {
  Check,
  DashboardQuery,
  NotificationRule,
  ThresholdCheck,
  StatusRuleDraft,
  TagRuleDraft,
  NotificationRuleDraft,
  DeadmanCheck,
} from 'src/types'
import {NotificationEndpoint} from 'src/client'

export const DEFAULT_CHECK_NAME = 'Name this check'
export const DEFAULT_NOTIFICATION_RULE_NAME = 'Name this notification rule'

export const CHECK_NAME_MAX_LENGTH = 68
export const DEFAULT_CHECK_CRON = '1h'
export const DEFAULT_CHECK_EVERY = '1h'
export const DEFAULT_CHECK_OFFSET = '0s'
export const DEFAULT_CHECK_REPORT_ZERO = false

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

export const query: DashboardQuery = {
  text: 'this is query',
  editMode: 'advanced',
  builderConfig: null,
  name: 'great q',
}

export const check1: Check = {
  id: '1',
  type: 'threshold',
  name: 'Amoozing check',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  query: query,
  status: 'active',
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: 'UNKNOWN',
      lowerBound: 20,
      allValues: false,
    },
  ],
}

export const check2: Check = {
  id: '2',
  type: 'threshold',
  name: 'Another check',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  query: query,
  status: 'active',
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: 'UNKNOWN',
      lowerBound: 20,
      allValues: false,
    },
  ],
}

export const checks: Array<Check> = [check1, check2]

export const newStatusRule: StatusRuleDraft = {
  id: '',
  value: {
    currentLevel: {
      operation: 'equal',
      level: 'WARN',
    },
    previousLevel: {
      operation: 'equal',
      level: 'OK',
    },
    period: '1h',
    count: 1,
  },
}

export const newTagRule: TagRuleDraft = {
  id: '',
  value: {
    key: '',
    value: '',
    operator: 'equal',
  },
}

export const newRule: NotificationRuleDraft = {
  id: '',
  notifyEndpointID: '1',
  type: 'slack',
  every: '',
  orgID: '',
  name: '',
  schedule: 'every',
  status: 'active',
  messageTemplate: '',
  tagRules: [newTagRule],
  statusRules: [newStatusRule],
  description: '',
}

export const endpoints: NotificationEndpoint[] = [
  {
    id: '1',
    orgID: '1',
    userID: '1',
    description: 'interrupt everyone at work',
    name: 'slack endpoint',
    status: 'active',
    type: 'slack',
  },
  {
    id: '2',
    orgID: '1',
    userID: '1',
    description: 'interrupt someone by email',
    name: 'smtp endpoint',
    status: 'active',
    type: 'smtp',
  },
  {
    id: '3',
    orgID: '1',
    userID: '1',
    description: 'interrupt someone by all means known to man',
    name: 'pagerditty endpoint',
    status: 'active',
    type: 'pagerduty',
  },
]

export const rule: NotificationRule = {
  id: '3',
  notifyEndpointID: '2',
  orgID: 'lala',
  createdAt: '2019-12-17T00:00',
  updatedAt: '2019-05-17T00:00',
  status: 'active',
  description: '',
  name: 'amazing notification rule',
  type: 'slack',
  every: '2d',
  offset: '5m',
  limitEvery: 1,
  limit: 5,
  tagRules: [],
  statusRules: [],
  channel: '#monitoring-team',
  messageTemplate: 'hello, this is a NotificationRule fixture speaking :)',
}
