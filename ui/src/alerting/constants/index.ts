import {
  Check,
  CheckType,
  DashboardQuery,
  QueryEditMode,
  CheckBase,
  NotificationRule,
  NotificationRuleBase,
  NotificationRuleType,
  CheckStatusLevel,
  ThresholdType,
} from 'src/types'

export const DEFAULT_CHECK_NAME = 'Name this check'
export const DEFAULT_NOTIFICATION_RULE_NAME = 'Name this notification rule'

export const query: DashboardQuery = {
  text: 'this is query',
  editMode: QueryEditMode.Advanced,
  builderConfig: null,
  name: 'great q',
}

export const check1: Check = {
  id: '1',
  type: CheckType.Threshold,
  name: 'Amoozing check',
  orgID: 'lala',
  createdAt: new Date('December 17, 2019'),
  updatedAt: new Date('April 17, 2019'),
  query: query,
  status: CheckBase.StatusEnum.Active,
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: CheckStatusLevel.WARN,
      allValues: false,
      type: ThresholdType.Greater,
    },
  ],
}

export const check2: Check = {
  id: '2',
  type: CheckType.Threshold,
  name: 'Another check',
  orgID: 'lala',
  createdAt: new Date('December 17, 2019'),
  updatedAt: new Date('April 17, 2019'),
  query: query,
  status: CheckBase.StatusEnum.Active,
  every: '2d',
  offset: '1m',
  tags: [{key: 'a', value: 'b'}],
  statusMessageTemplate: 'this is a great message template',
  thresholds: [
    {
      level: CheckStatusLevel.WARN,
      allValues: false,
      type: ThresholdType.Greater,
    },
  ],
}

export const checks: Array<Check> = [check1, check2]

export const notificationRule: NotificationRule = {
  id: '3',
  notifyEndpointID: '2',
  orgID: 'lala',
  createdAt: new Date('December 17, 2019'),
  updatedAt: new Date('April 17, 2019'),
  status: NotificationRuleBase.StatusEnum.Active,
  name: 'amazing notification rule',
  type: NotificationRuleType.Slack,
  every: '2d',
  offset: '5m',
  limitEvery: 1,
  limit: 5,
  tagRules: [],
  statusRules: [],
}
