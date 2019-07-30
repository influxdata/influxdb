import {
  Check,
  DashboardQuery,
  NotificationRule,
  GreaterThreshold,
  ThresholdCheck,
} from 'src/types'

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

export const newRule: NotificationRule = {
  id: '',
  type: 'slack',
  every: '',
  messageTemplate: '',
  orgID: '',
  name: '',
  status: 'active',
  tagRules: [],
  statusRules: [],
  description: 'description',
}

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
