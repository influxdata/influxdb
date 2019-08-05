import {
  Check,
  DashboardQuery,
  NotificationRule,
  GreaterThreshold,
  ThresholdCheck,
  StatusRuleItem,
  TagRuleItem,
  NotificationRuleBox,
} from 'src/types'
import {NotificationEndpoint} from 'src/client'

export const DEFAULT_CHECK_NAME = 'Name this check'
export const DEFAULT_NOTIFICATION_RULE_NAME = 'Name this notification rule'

export const DEFAULT_CHECK: Partial<ThresholdCheck> = {
  name: DEFAULT_CHECK_NAME,
  type: 'threshold',
  status: 'active',
  thresholds: [],
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
      level: 'WARN',
      allValues: false,
      type: 'greater',
    } as GreaterThreshold,
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
      level: 'WARN',
      allValues: false,
      type: 'greater',
    } as GreaterThreshold,
  ],
}

export const checks: Array<Check> = [check1, check2]

export const newStatusRule: StatusRuleItem = {
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

export const newTagRule: TagRuleItem = {
  id: '',
  value: {
    key: '',
    value: '',
    operator: 'equal',
  },
}

export const newRule: NotificationRuleBox = {
  id: '',
  notifyEndpointID: '3',
  type: 'slack',
  every: '',
  orgID: '',
  name: '',
  schedule: 'every',
  status: 'active',
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
  {
    id: '4',
    orgID: '1',
    userID: '1',
    description: 'interrupt someone with a webhook!',
    name: 'webhook endpoint',
    status: 'active',
    type: 'webhook',
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
